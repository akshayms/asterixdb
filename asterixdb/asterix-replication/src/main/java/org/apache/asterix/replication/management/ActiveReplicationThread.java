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
public class ActiveReplicationThread implements IReplicationThread {

    private static final Logger LOGGER = Logger.getLogger(ActiveReplicationThread.class.getName());
    private static final int INTIAL_BUFFER_SIZE = StorageUtil.getIntSizeInBytes(4, StorageUtil.StorageUnit.KILOBYTE);
    private static final int LOG_REPLICATION_END_HANKSHAKE_LOG_SIZE = 1;

    private final SocketChannel socketChannel;
    private final LogRecord remoteLog;
    private ByteBuffer inBuffer;
    private ByteBuffer outBuffer;
    private StreamingReplicationChannel replicationChannel;

    public ActiveReplicationThread(IReplicationChannel replicationChannel, SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
        this.replicationChannel = (StreamingReplicationChannel) replicationChannel;
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
                    case DELETE_FILE:
                        handleDeleteFile();
                        break;
                    case REPLICA_EVENT:
                        handleReplicaEvent();
                        break;
                    case GET_REPLICA_FILES:
                        handleGetReplicaFiles();
                        break;
                    case CREATE_INDEX:
                        handleIndexCreate();
                        break;
                    case GET_REPLICA_MAX_LSN:
                        handleGetReplicaMaxLSN();
                        break;

                    case REPLICATE_FILE:
                    case LSM_COMPONENT_PROPERTIES:
                    case FLUSH_INDEX:
                    default:
                        throw new IllegalStateException("Unknown replication request: " + replicationFunction.name());
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

    private void handleGetReplicaMaxLSN() throws IOException {
        long maxLNS = replicationChannel.getLogManager().getAppendLSN();
        outBuffer.clear();
        outBuffer.putLong(maxLNS);
        outBuffer.flip();
        NetworkingUtil.transferBufferToChannel(socketChannel, outBuffer);
    }

    private void handleIndexCreate() throws IOException {
        inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
        LSMIndexFileProperties asterixFileProperties = ReplicationProtocol.readFileReplicationRequest(inBuffer);
        String indexPath = ((StreamingReplicationChannel) replicationChannel).replicaResourcesManager.getIndexPath
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
        LOGGER.info("Deleting file " + fileProp.getFilePath());
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
//                        LOGGER.info("Persisting log: " + remoteLog.getLogRecordForDisplay());
                        try {
                            replicationChannel.logReplayManager.submit(remoteLog);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
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
                    // TODO: Start recovery in individual partitions
                    break;
                case LogType.FLUSH:
                    //LOGGER.info("Requesting a flush of a remote primary. Log: " + remoteLog.getLogRecordForDisplay
                    // ());
                    try {
                        replicationChannel.logReplayManager.submit(remoteLog);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
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
