package org.apache.asterix.replication.management;

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.config.IPropertiesProvider;
import org.apache.asterix.common.context.IndexInfo;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.ioopcallbacks.AbstractLSMIOOperationCallback;
import org.apache.asterix.common.replication.IReplicationChannel;
import org.apache.asterix.common.replication.IReplicationStrategy;
import org.apache.asterix.common.replication.IReplicationThread;
import org.apache.asterix.common.replication.ReplicaEvent;
import org.apache.asterix.common.storage.IndexFileProperties;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.LogSource;
import org.apache.asterix.common.transactions.LogType;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.common.utils.TransactionUtil;
import org.apache.asterix.replication.functions.ReplicaFilesRequest;
import org.apache.asterix.replication.functions.ReplicaIndexFlushRequest;
import org.apache.asterix.replication.functions.ReplicationProtocol;
import org.apache.asterix.replication.logging.RemoteLogMapping;
import org.apache.asterix.replication.storage.LSMComponentLSNSyncTask;
import org.apache.asterix.replication.storage.LSMComponentProperties;
import org.apache.asterix.replication.storage.LSMIndexFileProperties;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.util.StorageUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A replication thread is created per received replication request.
 */
public class PassiveReplicationThread implements IReplicationThread {

    private static final Logger LOGGER = Logger.getLogger(PassiveReplicationThread.class.getName());
    private static final int INTIAL_BUFFER_SIZE = StorageUtil.getIntSizeInBytes(4, StorageUtil.StorageUnit.KILOBYTE);
    private static final int LOG_REPLICATION_END_HANKSHAKE_LOG_SIZE = 1;

    private final SocketChannel socketChannel;
    private final LogRecord remoteLog;
    private ByteBuffer inBuffer;
    private ByteBuffer outBuffer;
    private ReplicationChannel replicationChannel;

    public PassiveReplicationThread(IReplicationChannel replicationChannel, SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
        this.replicationChannel = (ReplicationChannel) replicationChannel;
        inBuffer = ByteBuffer.allocate(INTIAL_BUFFER_SIZE);
        outBuffer = ByteBuffer.allocate(INTIAL_BUFFER_SIZE);
        remoteLog = new LogRecord();
    }

    @Override
    public void run() {
        Thread.currentThread().setName("Replication Thread");
        try {
            ReplicationProtocol.ReplicationRequestType replicationFunction = ReplicationProtocol.getRequestType(socketChannel,
                    inBuffer);
            while (replicationFunction != ReplicationProtocol.ReplicationRequestType.GOODBYE) {
                switch (replicationFunction) {
                    case REPLICATE_LOG:
                        handleLogReplication();
                        break;
                    case LSM_COMPONENT_PROPERTIES:
                        handleLSMComponentProperties();
                        break;
                    case REPLICATE_FILE:
                        handleReplicateFile();
                        break;
                    case DELETE_FILE:
                        handleDeleteFile();
                        break;
                    case REPLICA_EVENT:
                        handleReplicaEvent();
                        break;
                    case GET_REPLICA_MAX_LSN:
                        handleGetReplicaMaxLSN();
                        break;
                    case GET_REPLICA_FILES:
                        handleGetReplicaFiles();
                        break;
                    case FLUSH_INDEX:
                        handleFlushIndex();
                        break;
                    case CREATE_INDEX:
                        handleIndexCreate();
                        break;
                    default:
                        throw new IllegalStateException("Unknown replication request");
                }
                replicationFunction = ReplicationProtocol.getRequestType(socketChannel, inBuffer);
            }
        } catch (Exception e) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.log(Level.WARNING, "Unexpectedly error during replication.", e);
            }
        } finally {
            if (socketChannel.isOpen()) {
                try {
                    socketChannel.close();
                } catch (IOException e) {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.log(Level.WARNING, "Filed to close replication socket.", e);
                    }
                }
            }
        }
    }

    private void handleIndexCreate() throws IOException {
        inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
        LSMIndexFileProperties asterixFileProperties = ReplicationProtocol.readFileReplicationRequest(inBuffer);

        String indexPath = ((ReplicationChannel) replicationChannel).replicaResourcesManager.getIndexPath
                (asterixFileProperties);
        String replicaFilePath = indexPath + File.separator + asterixFileProperties.getFileName();
        LOGGER.info("Creating replica file: " + replicaFilePath);
        File destFile = new File(replicaFilePath);
        destFile.createNewFile();
        long fileSize = asterixFileProperties.getFileSize();

        RandomAccessFile fileOutputStream = new RandomAccessFile(destFile, "rw");
        FileChannel fileChannel = fileOutputStream.getChannel();
        fileOutputStream.setLength(fileSize);
        LOGGER.info("Downlading File : " + destFile + " with size: " + fileSize);
        NetworkingUtil.downloadFile(fileChannel, socketChannel);
        fileChannel.force(true);
        if (asterixFileProperties.requiresAck()) {
            ReplicationProtocol.sendAck(socketChannel);
        }
    }

    private void handleFlushIndex() throws IOException {
        inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
        //read which indexes are requested to be flushed from remote replica
        ReplicaIndexFlushRequest request = ReplicationProtocol.readReplicaIndexFlushRequest(inBuffer);
        Set<Long> requestedIndexesToBeFlushed = request.getLaggingRescouresIds();

        /**
         * check which indexes can be flushed (open indexes) and which cannot be
         * flushed (closed or have empty memory component).
         */
        IDatasetLifecycleManager datasetLifeCycleManager = replicationChannel.getAppRuntimeContextProvider()
                .getDatasetLifecycleManager();
        List<IndexInfo> openIndexesInfo = datasetLifeCycleManager.getOpenIndexesInfo();
        Set<Integer> datasetsToForceFlush = new HashSet<>();
        for (IndexInfo iInfo : openIndexesInfo) {
            if (requestedIndexesToBeFlushed.contains(iInfo.getResourceId())) {
                AbstractLSMIOOperationCallback ioCallback = (AbstractLSMIOOperationCallback) iInfo.getIndex()
                        .getIOOperationCallback();
                //if an index has a pending flush, then the request to flush it will succeed.
                if (ioCallback.hasPendingFlush()) {
                    //remove index to indicate that it will be flushed
                    requestedIndexesToBeFlushed.remove(iInfo.getResourceId());
                } else if (!((AbstractLSMIndex) iInfo.getIndex()).isCurrentMutableComponentEmpty()) {
                    /**
                     * if an index has something to be flushed, then the request to flush it
                     * will succeed and we need to schedule it to be flushed.
                     */
                    datasetsToForceFlush.add(iInfo.getDatasetId());
                    //remove index to indicate that it will be flushed
                    requestedIndexesToBeFlushed.remove(iInfo.getResourceId());
                }
            }
        }

        //schedule flush for datasets requested to be flushed
        for (int datasetId : datasetsToForceFlush) {
            datasetLifeCycleManager.flushDataset(datasetId, true);
        }

        //the remaining indexes in the requested set are those which cannot be flushed.
        //respond back to the requester that those indexes cannot be flushed
        ReplicaIndexFlushRequest laggingIndexesResponse = new ReplicaIndexFlushRequest(requestedIndexesToBeFlushed);
        outBuffer = ReplicationProtocol.writeGetReplicaIndexFlushRequest(outBuffer, laggingIndexesResponse);
        NetworkingUtil.transferBufferToChannel(socketChannel, outBuffer);
    }

    private void handleLSMComponentProperties() throws IOException {
        inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
        LSMComponentProperties lsmCompProp = ReplicationProtocol.readLSMPropertiesRequest(inBuffer);
        //create mask to indicate that this component is not valid yet
        replicationChannel.replicaResourcesManager.createRemoteLSMComponentMask(lsmCompProp);
        replicationChannel.lsmComponentId2PropertiesMap.put(lsmCompProp.getComponentId(),
                lsmCompProp);
    }

    private void handleReplicateFile() throws IOException {
        inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
        LSMIndexFileProperties afp = ReplicationProtocol.readFileReplicationRequest(inBuffer);

        //get index path
        String indexPath = ((ReplicationChannel) replicationChannel).replicaResourcesManager.getIndexPath(afp);
        String replicaFilePath = indexPath + File.separator + afp.getFileName();

        //create file
        File destFile = new File(replicaFilePath);
        destFile.createNewFile();

        try (RandomAccessFile fileOutputStream = new RandomAccessFile(destFile, "rw");
             FileChannel fileChannel = fileOutputStream.getChannel()) {
            fileOutputStream.setLength(afp.getFileSize());
            NetworkingUtil.downloadFile(fileChannel, socketChannel);
            fileChannel.force(true);

            if (afp.requiresAck()) {
                ReplicationProtocol.sendAck(socketChannel);
            }
            if (afp.isLSMComponentFile()) {
                String componentId = LSMComponentProperties.getLSMComponentID(afp.getFilePath());
                if (afp.getLSNByteOffset() > AbstractLSMIOOperationCallback.INVALID) {
                    LSMComponentLSNSyncTask syncTask = new LSMComponentLSNSyncTask(componentId,
                            destFile.getAbsolutePath(), afp.getLSNByteOffset());
                    replicationChannel.lsmComponentRemoteLSN2LocalLSNMappingTaskQ.offer(syncTask);
                } else {
                    ((ReplicationChannel)replicationChannel).updateLSMComponentRemainingFiles(componentId);
                }
            } else {
                //index metadata file
                replicationChannel.replicaResourcesManager.initializeReplicaIndexLSNMap(indexPath, replicationChannel
                        .getLogManager()
                        .getAppendLSN());
            }
        }
    }

    private void handleGetReplicaMaxLSN() throws IOException {
        long maxLNS = replicationChannel.getLogManager().getAppendLSN();
        outBuffer.clear();
        outBuffer.putLong(maxLNS);
        outBuffer.flip();
        NetworkingUtil.transferBufferToChannel(socketChannel, outBuffer);
    }

    private void handleGetReplicaFiles() throws IOException {
        inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
        ReplicaFilesRequest request = ReplicationProtocol.readReplicaFileRequest(inBuffer);

        LOGGER.info("Get replica files request from: " + socketChannel.getRemoteAddress() + " Request: " + request);

        LSMIndexFileProperties fileProperties = new LSMIndexFileProperties();

        List<String> filesList;
        Set<Integer> partitionIds = request.getPartitionIds();
        Set<String> requesterExistingFiles = request.getExistingFiles();
        Map<Integer, ClusterPartition> clusterPartitions = ((IPropertiesProvider) replicationChannel.
        appContextProvider
                .getAppContext()).getMetadataProperties().getClusterPartitions();

        final IReplicationStrategy repStrategy = replicationChannel.replicationProperties.getReplicationStrategy();
        // Flush replicated datasets to generate the latest LSM components
        replicationChannel.dsLifecycleManager.flushDataset(repStrategy);
        for (Integer partitionId : partitionIds) {
            ClusterPartition partition = clusterPartitions.get(partitionId);
            filesList = replicationChannel.replicaResourcesManager.getPartitionIndexesFiles(partition.getPartitionId
                    (), false);
            //start sending files
            for (String filePath : filesList) {
                // Send only files of datasets that are replciated.

                if (Files.isDirectory(Paths.get(filePath))) {
                    LOGGER.info("Not a regular file! " + filePath);
                    continue;
                }

                IndexFileProperties indexFileRef = replicationChannel.localResourceRep.getIndexFileRef(filePath);
                if (!repStrategy.isMatchForFailover(indexFileRef.getDatasetId())) {
                    continue;
                }

                String relativeFilePath = StoragePathUtil.getIndexFileRelativePath(filePath);
                //if the file already exists on the requester, skip it
                if (!requesterExistingFiles.contains(relativeFilePath)) {
                    try (RandomAccessFile fromFile = new RandomAccessFile(filePath, "r");
                         FileChannel fileChannel = fromFile.getChannel();) {
                        long fileSize = fileChannel.size();
                        fileProperties.initialize(filePath, fileSize, partition.getNodeId(), false,
                                AbstractLSMIOOperationCallback.INVALID, false);
                        outBuffer = ReplicationProtocol.writeFileReplicationRequest(outBuffer, fileProperties,
                                ReplicationProtocol.ReplicationRequestType.REPLICATE_FILE);

                        //send file info
                        NetworkingUtil.transferBufferToChannel(socketChannel, outBuffer);

                        //transfer file
                        NetworkingUtil.sendFile(fileChannel, socketChannel);
                    }
                }
            }
        }

        //send goodbye (end of files)
        ReplicationProtocol.sendGoodbye(socketChannel);
    }

    private void handleReplicaEvent() throws IOException {
        inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
        ReplicaEvent event = ReplicationProtocol.readReplicaEventRequest(inBuffer);
        replicationChannel.replicationManager.reportReplicaEvent(event);
    }

    private void handleDeleteFile() throws IOException {
        inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
        LSMIndexFileProperties fileProp = ReplicationProtocol.readFileReplicationRequest(inBuffer);
        replicationChannel.replicaResourcesManager.deleteIndexFile(fileProp);
        if (fileProp.requiresAck()) {
            ReplicationProtocol.sendAck(socketChannel);
        }
    }

    public void handleLogReplication() throws IOException, ACIDException {
        //set initial buffer size to a log buffer page size
        inBuffer = ByteBuffer.allocate(replicationChannel.getLogManager().getLogPageSize());
        while (true) {
            //read a batch of logs
            inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
            //check if it is end of handshake (a single byte log)
            if (inBuffer.remaining() == LOG_REPLICATION_END_HANKSHAKE_LOG_SIZE) {
                break;
            }

            processLogsBatch(inBuffer);
        }
    }

    private void processLogsBatch(ByteBuffer buffer) throws ACIDException {
        while (buffer.hasRemaining()) {
            //get rid of log size
            inBuffer.getInt();
            //Deserialize log
            remoteLog.readRemoteLog(inBuffer);
            remoteLog.setLogSource(LogSource.REMOTE);

            switch (remoteLog.getLogType()) {
                case LogType.UPDATE:
                case LogType.ENTITY_COMMIT:
                case LogType.UPSERT_ENTITY_COMMIT:
                    //if the log partition belongs to a partitions hosted on this node, replicate it
                    if (replicationChannel.nodeHostedPartitions.contains(remoteLog.getResourcePartition())) {
                        replicationChannel.getLogManager().log(remoteLog);
                        LOGGER.info("Persisting log: " + remoteLog.getLogRecordForDisplay());
                        replicationChannel.streamingReplicationJobManager.submit(remoteLog);
                    }
                    break;
                case LogType.JOB_COMMIT:
                case LogType.ABORT:
                    LogRecord jobTerminationLog = new LogRecord();
                    TransactionUtil.formJobTerminateLogRecord(jobTerminationLog, remoteLog.getJobId(),
                            remoteLog.getLogType() == LogType.JOB_COMMIT);
                    jobTerminationLog.setReplicationThread(this);
                    jobTerminationLog.setLogSource(LogSource.REMOTE);
                    replicationChannel.getLogManager().log(jobTerminationLog);
                    break;
                case LogType.FLUSH:
                    LOGGER.info("Flush record: " + remoteLog.getLogRecordForDisplay());
                    //store mapping information for flush logs to use them in incoming LSM components.
                    RemoteLogMapping flushLogMap = new RemoteLogMapping();
                    flushLogMap.setRemoteNodeID(remoteLog.getNodeId());
                    flushLogMap.setRemoteLSN(remoteLog.getLSN());
                    replicationChannel.getLogManager().log(remoteLog);
                    //the log LSN value is updated by logManager.log(.) to a local value
                    flushLogMap.setLocalLSN(remoteLog.getLSN());
                    flushLogMap.numOfFlushedIndexes.set(remoteLog.getNumOfFlushedIndexes());
                    replicationChannel.replicaUniqueLSN2RemoteMapping.put(flushLogMap.getNodeUniqueLSN(), flushLogMap);
                    synchronized (replicationChannel.flushLogslock) {
                        replicationChannel.flushLogslock.notify();
                    }
                    break;
                default:
                    LOGGER.severe("Unsupported LogType: " + remoteLog.getLogType());
            }
        }
    }

    /**
     * this method is called sequentially by LogPage (notifyReplicationTerminator)
     * for JOB_COMMIT and JOB_ABORT log types.
     */
    @Override
    public void notifyLogReplicationRequester(LogRecord logRecord) {
        replicationChannel.pendingNotificationRemoteLogsQ.offer
                (logRecord);
    }

    @Override
    public SocketChannel getReplicationClientSocket() {
        return socketChannel;
    }
}
