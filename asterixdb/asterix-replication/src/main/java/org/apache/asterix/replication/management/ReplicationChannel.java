/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.replication.management;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.api.ILocalResourceMetadata;
import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.config.IPropertiesProvider;
import org.apache.asterix.common.config.ReplicationProperties;
import org.apache.asterix.common.context.IndexInfo;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.ioopcallbacks.AbstractLSMIOOperationCallback;
import org.apache.asterix.common.replication.IReplicaResourcesManager;
import org.apache.asterix.common.replication.IReplicationChannel;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.replication.IReplicationThread;
import org.apache.asterix.common.replication.ReplicaEvent;
import org.apache.asterix.common.transactions.IAppRuntimeContextProvider;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.LogSource;
import org.apache.asterix.common.transactions.LogType;
import org.apache.asterix.common.utils.TransactionUtil;
import org.apache.asterix.replication.functions.ReplicaFilesRequest;
import org.apache.asterix.replication.functions.ReplicaIndexFlushRequest;
import org.apache.asterix.replication.functions.ReplicationJob;
import org.apache.asterix.replication.functions.ReplicationProtocol;
import org.apache.asterix.replication.functions.ReplicationProtocol.ReplicationRequestType;
import org.apache.asterix.replication.logging.RemoteLogMapping;
import org.apache.asterix.replication.storage.LSMComponentLSNSyncTask;
import org.apache.asterix.replication.storage.LSMComponentProperties;
import org.apache.asterix.replication.storage.LSMIndexFileProperties;
import org.apache.asterix.replication.storage.ReplicaResourcesManager;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.asterix.transaction.management.service.logging.LogReader;
import org.apache.asterix.transaction.management.service.recovery.RecoveryManager;
import org.apache.asterix.transaction.management.service.recovery.TxnId;
import org.apache.asterix.transaction.management.service.transaction.TransactionManagementConstants;
import org.apache.asterix.transaction.management.service.transaction.TransactionSubsystem;
import org.apache.hive.com.esotericsoftware.kryo.serializers.DefaultSerializers;
import org.apache.hyracks.api.application.INCApplicationContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobIdFactory;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IMetaDataPageManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.common.file.LocalResource;
import org.apache.hyracks.util.StorageUtil;
import org.apache.hyracks.util.StorageUtil.StorageUnit;
import org.codehaus.groovy.transform.sc.ListOfExpressionsExpression;

import javax.jdo.annotations.Persistent;

/**
 * This class is used to receive and process replication requests from remote replicas or replica events from CC
 */
public class ReplicationChannel extends Thread implements IReplicationChannel {

    private static final Logger LOGGER = Logger.getLogger(ReplicationChannel.class.getName());
    private static final int LOG_REPLICATION_END_HANKSHAKE_LOG_SIZE = 1;
    private final ExecutorService replicationThreads;
    private final String localNodeID;
    private final ILogManager logManager;
    private final ReplicaResourcesManager replicaResourcesManager;
    private SocketChannel socketChannel = null;
    private ServerSocketChannel serverSocketChannel = null;
    private final IReplicationManager replicationManager;
    private final ReplicationProperties replicationProperties;
    private final IAppRuntimeContextProvider appContextProvider;
    private static final int INTIAL_BUFFER_SIZE = StorageUtil.getSizeInBytes(4, StorageUnit.KILOBYTE);
    private final LinkedBlockingQueue<LSMComponentLSNSyncTask> lsmComponentRemoteLSN2LocalLSNMappingTaskQ;
    private final LinkedBlockingQueue<LogRecord> pendingNotificationRemoteLogsQ;
    private final Map<String, LSMComponentProperties> lsmComponentId2PropertiesMap;
    private final Map<String, RemoteLogMapping> replicaUniqueLSN2RemoteMapping;
    private final LSMComponentsSyncService lsmComponentLSNMappingService;
    private final Set<Integer> nodeHostedPartitions;
    private final ReplicationNotifier replicationNotifier;
    private final Object flushLogslock = new Object();
    private final ITransactionSubsystem txnSubSystem;
    private final PersistentLocalResourceRepository localResourceRepository;
    private final ExecutorService materializationThreads;

    // TODO: Change to (RID, PKHashValue, lastOpLSN)
    public Map<Long, Map<Integer, Long>> inflightOps;
    private OpTrackerProfiler profiler;
    private Map<Integer, Integer> PKstatus; // Why?
    private int opCounter = 0;


    public ReplicationChannel(String nodeId, ReplicationProperties replicationProperties, ILogManager logManager,
            IReplicaResourcesManager replicaResoucesManager, IReplicationManager replicationManager,
            INCApplicationContext appContext, IAppRuntimeContextProvider asterixAppRuntimeContextProvider) {
        this.logManager = logManager;
        this.localNodeID = nodeId;
        this.replicaResourcesManager = (ReplicaResourcesManager) replicaResoucesManager;
        this.replicationManager = replicationManager;
        this.replicationProperties = replicationProperties;
        this.appContextProvider = asterixAppRuntimeContextProvider;
        lsmComponentRemoteLSN2LocalLSNMappingTaskQ = new LinkedBlockingQueue<>();
        pendingNotificationRemoteLogsQ = new LinkedBlockingQueue<>();
        lsmComponentId2PropertiesMap = new ConcurrentHashMap<>();
        replicaUniqueLSN2RemoteMapping = new ConcurrentHashMap<>();
        lsmComponentLSNMappingService = new LSMComponentsSyncService();
        replicationNotifier = new ReplicationNotifier();
        replicationThreads = Executors.newCachedThreadPool(appContext.getThreadFactory());
        Map<String, ClusterPartition[]> nodePartitions =
                ((IPropertiesProvider) asterixAppRuntimeContextProvider.getAppContext()).getMetadataProperties()
                        .getNodePartitions();
        Set<String> nodeReplicationClients = replicationProperties.getNodeReplicationClients(nodeId);
        List<Integer> clientsPartitions = new ArrayList<>();
        for (String clientId : nodeReplicationClients) {
            for (ClusterPartition clusterPartition : nodePartitions.get(clientId)) {
                clientsPartitions.add(clusterPartition.getPartitionId());
            }
        }
        nodeHostedPartitions = new HashSet<>(clientsPartitions.size());
        nodeHostedPartitions.addAll(clientsPartitions);
        txnSubSystem = logManager.getTransactionSubsystem();
        localResourceRepository = (PersistentLocalResourceRepository) txnSubSystem
                .getAsterixAppRuntimeContextProvider().getLocalResourceRepository();

        inflightOps = new HashMap<>();
        PKstatus = new HashMap<>();
        LOGGER.info("Starting Hot-Standby OpTracker Thread");
        this.profiler = new OpTrackerProfiler(60000);
        this.profiler.start();
        materializationThreads = Executors.newCachedThreadPool(appContext.getThreadFactory());
    }

    public synchronized String printOpStatus() {
        StringBuilder sb = new StringBuilder();
        sb.append("== OP STATUS == NC: " + replicationManager.getNodeId());
        sb.append("\n");
        inflightOps.entrySet().stream()
                .peek(kv -> sb.append(" RID: ").append(kv.getKey()).append("\n"))
                .flatMap(kv -> kv.getValue().entrySet().stream())
                .forEach(kv -> sb.append("PK:").append(kv.getKey()).append(" Last LSN: ").append(kv.getValue())
                        .append("\n"));

        long total = inflightOps.values().stream()
                .flatMap(m -> m.values().stream())
                .count();
        sb.append("====TOTAL: " + total + "=====");
        // TODO: Add a counter of active ops that are being tracked.
        return sb.toString();
    }

    @Override
    public void run() {
        Thread.currentThread().setName("Replication Channel Thread");

        String nodeIP = replicationProperties.getReplicaIPAddress(localNodeID);
        int dataPort = replicationProperties.getDataReplicationPort(localNodeID);
        try {
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(true);
            InetSocketAddress replicationChannelAddress =
                    new InetSocketAddress(InetAddress.getByName(nodeIP), dataPort);
            serverSocketChannel.socket().bind(replicationChannelAddress);
            lsmComponentLSNMappingService.start();
            replicationNotifier.start();
            LOGGER.log(Level.INFO, "opened Replication Channel @ IP Address: " + nodeIP + ":" + dataPort);

            //start accepting replication requests
            while (true) {
                socketChannel = serverSocketChannel.accept();
                socketChannel.configureBlocking(true);
                //start a new thread to handle the request
                LOGGER.info("New replication thread requested, creating new replication thread");
                replicationThreads.execute(new ReplicationThread(socketChannel));
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Could not open replication channel @ IP Address: " + nodeIP + ":" + dataPort, e);
        }
    }

    private void updateLSMComponentRemainingFiles(String lsmComponentId) throws IOException {
        LSMComponentProperties lsmCompProp = lsmComponentId2PropertiesMap.get(lsmComponentId);
        int remainingFile = lsmCompProp.markFileComplete();

        //clean up when all the LSM component files have been received.
        if (remainingFile == 0) {
            if (lsmCompProp.getOpType() == LSMOperationType.FLUSH && lsmCompProp.getReplicaLSN() != null
                    && replicaUniqueLSN2RemoteMapping.containsKey(lsmCompProp.getNodeUniqueLSN())) {
                int remainingIndexes =
                        replicaUniqueLSN2RemoteMapping.get(lsmCompProp.getNodeUniqueLSN()).numOfFlushedIndexes
                                .decrementAndGet();
                if (remainingIndexes == 0) {
                    /**
                     * Note: there is a chance that this will never be removed because some
                     * index in the dataset was not flushed because it is empty. This could
                     * be solved by passing only the number of successfully flushed indexes.
                     */
                    replicaUniqueLSN2RemoteMapping.remove(lsmCompProp.getNodeUniqueLSN());
                }
            }

            //delete mask to indicate that this component is now valid.
            replicaResourcesManager.markLSMComponentReplicaAsValid(lsmCompProp);
            lsmComponentId2PropertiesMap.remove(lsmComponentId);
            LOGGER.log(Level.INFO, "Completed LSMComponent " + lsmComponentId + " Replication.");
        }
    }

    @Override
    public void close() throws IOException {
        if (!serverSocketChannel.isOpen()) {
            serverSocketChannel.close();
            LOGGER.log(Level.INFO, "Replication channel closed.");
        }
    }

    /**
     * A replication thread is created per received replication request.
     */
    private class ReplicationThread implements IReplicationThread {
        private final SocketChannel socketChannel;
        //private final LogRecord remoteLog;
        private LogRecord remoteLog;
        private ByteBuffer inBuffer;
        private ByteBuffer outBuffer;
        private IDatasetLifecycleManager datasetLifecycleManager;

        public ReplicationThread(SocketChannel socketChannel) {
            this.socketChannel = socketChannel;
            inBuffer = ByteBuffer.allocate(INTIAL_BUFFER_SIZE);
            outBuffer = ByteBuffer.allocate(INTIAL_BUFFER_SIZE);
            remoteLog = new LogRecord();
            datasetLifecycleManager = appContextProvider.getDatasetLifecycleManager();
            LOGGER.info("New replication thread started!");
        }

        @Override
        public void run() {
            Thread.currentThread().setName("Replication Thread");
            try {
                ReplicationRequestType replicationFunction =
                        ReplicationProtocol.getRequestType(socketChannel, inBuffer);
                while (replicationFunction != ReplicationRequestType.GOODBYE) {
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

        private void handleFlushIndex() throws IOException {
            inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
            //read which indexes are requested to be flushed from remote replica
            ReplicaIndexFlushRequest request = ReplicationProtocol.readReplicaIndexFlushRequest(inBuffer);
            Set<Long> requestedIndexesToBeFlushed = request.getLaggingRescouresIds();

            /**
             * check which indexes can be flushed (open indexes) and which cannot be
             * flushed (closed or have empty memory component).
             */
            IDatasetLifecycleManager datasetLifeCycleManager = appContextProvider.getDatasetLifecycleManager();
            List<IndexInfo> openIndexesInfo = datasetLifeCycleManager.getOpenIndexesInfo();
            Set<Integer> datasetsToForceFlush = new HashSet<>();
            for (IndexInfo iInfo : openIndexesInfo) {
                if (requestedIndexesToBeFlushed.contains(iInfo.getResourceId())) {
                    AbstractLSMIOOperationCallback ioCallback =
                            (AbstractLSMIOOperationCallback) iInfo.getIndex().getIOOperationCallback();
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
            replicaResourcesManager.createRemoteLSMComponentMask(lsmCompProp);
            lsmComponentId2PropertiesMap.put(lsmCompProp.getComponentId(), lsmCompProp);
        }

        private void handleReplicateFile() throws IOException {
            inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
            LSMIndexFileProperties afp = ReplicationProtocol.readFileReplicationRequest(inBuffer);

            //get index path
            String indexPath = replicaResourcesManager.getIndexPath(afp);
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
                        lsmComponentRemoteLSN2LocalLSNMappingTaskQ.offer(syncTask);
                    } else {
                        updateLSMComponentRemainingFiles(componentId);
                    }
                } else {
                    //index metadata file
                    replicaResourcesManager.initializeReplicaIndexLSNMap(indexPath, logManager.getAppendLSN());
                }
            }
        }

        private void handleGetReplicaMaxLSN() throws IOException {
            long maxLNS = logManager.getAppendLSN();
            outBuffer.clear();
            outBuffer.putLong(maxLNS);
            outBuffer.flip();
            NetworkingUtil.transferBufferToChannel(socketChannel, outBuffer);
        }

        private void handleGetReplicaFiles() throws IOException {
            inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
            ReplicaFilesRequest request = ReplicationProtocol.readReplicaFileRequest(inBuffer);

            LSMIndexFileProperties fileProperties = new LSMIndexFileProperties();

            List<String> filesList;
            Set<String> replicaIds = request.getReplicaIds();
            Set<String> requesterExistingFiles = request.getExistingFiles();
            Map<String, ClusterPartition[]> nodePartitions =
                    ((IPropertiesProvider) appContextProvider.getAppContext()).getMetadataProperties()
                            .getNodePartitions();
            for (String replicaId : replicaIds) {
                //get replica partitions
                ClusterPartition[] replicaPatitions = nodePartitions.get(replicaId);
                for (ClusterPartition partition : replicaPatitions) {
                    filesList = replicaResourcesManager.getPartitionIndexesFiles(partition.getPartitionId(), false);
                    //start sending files
                    for (String filePath : filesList) {
                        String relativeFilePath = PersistentLocalResourceRepository.getResourceRelativePath(filePath);
                        //if the file already exists on the requester, skip it
                        if (!requesterExistingFiles.contains(relativeFilePath)) {
                            try (RandomAccessFile fromFile = new RandomAccessFile(filePath, "r");
                                    FileChannel fileChannel = fromFile.getChannel();) {
                                long fileSize = fileChannel.size();
                                fileProperties.initialize(filePath, fileSize, replicaId, false,
                                        AbstractLSMIOOperationCallback.INVALID, false);
                                outBuffer = ReplicationProtocol.writeFileReplicationRequest(outBuffer, fileProperties,
                                        ReplicationRequestType.REPLICATE_FILE);

                                //send file info
                                NetworkingUtil.transferBufferToChannel(socketChannel, outBuffer);
                                //transfer file
                                NetworkingUtil.sendFile(fileChannel, socketChannel);
                            }
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
            replicationManager.reportReplicaEvent(event);
        }

        private void handleDeleteFile() throws IOException {
            inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
            LSMIndexFileProperties fileProp = ReplicationProtocol.readFileReplicationRequest(inBuffer);
            replicaResourcesManager.deleteIndexFile(fileProp);
            if (fileProp.requiresAck()) {
                ReplicationProtocol.sendAck(socketChannel);
            }
        }

        private void handleLogReplication() throws IOException, ACIDException {
            //set initial buffer size to a log buffer page size
            inBuffer = ByteBuffer.allocate(logManager.getLogPageSize());
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

        private void updatePKStatus() {
//            int PKHashValue = remoteLog.getPKHashValue();
//            PKstatus.putIfAbsent(PKHashValue, 0);
//            switch (remoteLog.getLogType()) {
//                case LogType.ENTITY_COMMIT:
//                case LogType.ABORT:c
//                case LogType.JOB_COMMIT:
//            }
        }

        private synchronized void updateLocalOpTracker() {
            long resourceId = remoteLog.getDatasetId();
            int PKHashValue = remoteLog.getPKHashValue();
            long lastLSN = remoteLog.getLSN();

            updatePKStatus();

            Map<Integer, Long> entityLocks = null;
            if (!inflightOps.containsKey(resourceId)) {
                entityLocks = new HashMap<>();
                inflightOps.put(resourceId, entityLocks);
            } else {
                entityLocks = inflightOps.get(resourceId);
            }
            if (entityLocks.containsKey(PKHashValue)) {
                if (remoteLog.getLogType() == LogType.ENTITY_COMMIT || remoteLog.getLogType() == LogType.JOB_COMMIT) {
                    LOGGER.info("Current last LSN before entity commit for PK: " + PKHashValue + ", LSN: " + entityLocks
                            .get(PKHashValue) + " logtype: " + remoteLog.getLogType());
                    LOGGER.info("Received EC for RID: " + resourceId + " PK: " + PKHashValue + "with LSN: " +
                            entityLocks.get(PKHashValue));
                    long removedLSN = entityLocks.remove(PKHashValue);
                    LOGGER.info("Removing PKHash, returned: " + removedLSN);
                    LOGGER.info(printOpStatus());
                } else {
                    LOGGER.info("Changing RID: " +resourceId+ ", PK: " + PKHashValue + " from " + entityLocks.get
                            (PKHashValue));
                    entityLocks.put(PKHashValue, lastLSN);
                }
            }
            else {
                LOGGER.info("New operation on RID: " + resourceId + ", PK: " + PKHashValue + " at LSN: " + lastLSN);
                entityLocks.put(PKHashValue, lastLSN);
                // TODO: update the print statement to include resource ID changes.
            }
            // TODO: Remove entities from the tracker on job completion.
            // Check all the PKHashValues that have received entity commit and compare them when doing rollback.
        }

        private void materialize(LogRecord remoteLog) throws HyracksDataException, ACIDException {
            LOGGER.info("Materializing: " + remoteLog.getLogRecordForDisplay());

            IAsterixAppRuntimeContextProvider appRuntimeContext =
                    txnSubSystem.getAsterixAppRuntimeContextProvider();
            IDatasetLifecycleManager datasetLifecycleManager = appRuntimeContext.getDatasetLifecycleManager();

            ILSMIndex index = null;
            ILocalResourceMetadata localResourceMetadata = null;

            long resourceId = remoteLog.getResourceId();
            Map<Long, LocalResource> resourceMap = localResourceRepository.loadAndGetAllResources();
            LocalResource localResource = resourceMap.get(resourceId);
            ITransactionContext txnCtx = null;
            try {
                txnCtx = txnSubSystem.getTransactionManager().getTransactionContext(new JobId(remoteLog
                        .getJobId()), true);
            } catch (ACIDException e) {
                e.printStackTrace();
                LOGGER.severe("REPL: TXN CONTEXT NOT FOUND!!!");
            }

            txnSubSystem.getLockManager().lock(new DatasetId(remoteLog.getDatasetId()), remoteLog.getPKHashValue(),
                    TransactionManagementConstants.LockManagerConstants.LockMode.X, txnCtx);

            //JobId jobID = org.apache.asterix.transaction.management.service.transaction.JobIdFactory.generateJobId();

            if (localResource == null) {
                throw new HyracksDataException("Local resource not found!");
            }

            // TODO: Can this be created at a different time?
            String partitionIODevicePath = localResourceRepository.getPartitionPath(localResource.getPartition());
            String resourceAbsolutePath =
                    partitionIODevicePath + File.separator + localResource.getResourceName();
            localResource.setResourcePath(resourceAbsolutePath);

            // TODO: Log this information. Can this operation be done at a different time?
            index = (ILSMIndex) datasetLifecycleManager.get(resourceAbsolutePath);
            if (index == null) {
                localResourceMetadata = (ILocalResourceMetadata) localResource.getResourceObject();
                index = localResourceMetadata.createIndexInstance(appRuntimeContext,
                        resourceAbsolutePath, localResource.getPartition(),
                        localResourceRepository.getIODeviceNum(localResource.getPartition()));
                datasetLifecycleManager.register(resourceAbsolutePath, index);
                datasetLifecycleManager.open(resourceAbsolutePath);
                // must be closed for each job ?
            }
            if (remoteLog.getLogType() == LogType.UPDATE) {
                // Print new value bytes.
                ITupleReference tuple = remoteLog.getNewValue();
                int size = remoteLog.getPKValueSize();
                RecoveryManager.redo(remoteLog, datasetLifecycleManager);
            }
            else {

            }
            datasetLifecycleManager.close(resourceAbsolutePath);

            txnSubSystem.getLockManager().unlock(new DatasetId(remoteLog.getDatasetId()), remoteLog.getPKHashValue(),
                    TransactionManagementConstants.LockManagerConstants.LockMode.X, txnCtx);
            //verify(remoteLog);
        }

        private void verify(LogRecord remoteLog) {
            ILogReader reader = logManager.getLogReader(false);
            try {
                reader.initializeScan(remoteLog.getLSN());
                ILogRecord record = reader.next();
                LOGGER.info("=== RETRIEVED RECORD ==== ");
                LOGGER.info(record.getLogRecordForDisplay());
                reader.close();
            } catch (ACIDException e) {
                e.printStackTrace();
            }
        }

        private void processLogsBatch(ByteBuffer buffer) throws ACIDException {
            int upCount, ecCount, upsertCount, commitCount, abortCount, flushCount, unknown;
            upCount = ecCount = upsertCount = commitCount = abortCount = flushCount = unknown = 0;
            ILSMIndex index = null;
            ILocalResourceMetadata localResourceMetadata = null;


            while (buffer.hasRemaining()) {
                //get rid of log size
                inBuffer.getInt();
                //Deserialize log
                //remoteLog = new LogRecord();
                remoteLog.readRemoteLog(inBuffer);
                remoteLog.setLogSource(LogSource.REMOTE);
                LOGGER.info("Replication Channel log recv :" + remoteLog.getLogRecordForDisplay());
                switch (remoteLog.getLogType()) {
                    case LogType.UPDATE:
                        upCount++;
                    case LogType.ENTITY_COMMIT:
                        ecCount++;
                    case LogType.UPSERT_ENTITY_COMMIT:
                        upsertCount++;
                        //if the log partition belongs to a partitions hosted on this node, replicate it
                        //LOGGER.info("LOG IS: " + remoteLog.getLogRecordForDisplay());
                        if (nodeHostedPartitions.contains(remoteLog.getResourcePartition())) {
                            LOGGER.info("Before LSN: " + remoteLog.getLSN());
                            // TODO: What happens on failure here?
                            logManager.log(remoteLog);
                            LOGGER.info("After LSN: " + remoteLog.getLSN());
                            // TODO: What happens on failure here?
                            if (remoteLog.getLogType() == LogType.UPDATE || remoteLog.getLogType() == LogType.ENTITY_COMMIT) {
                                try {
//                                    if (remoteLog.getPKHashValue() == 1399859334 &&
//                                            remoteLog.getLogType() == LogType.UPDATE) {
//                                        opCounter++;
//                                        if (opCounter == 2) {
//                                            LOGGER.info("REPL: Failing operation on ID 1");
//                                            throw new ACIDException("Half transaction hook");
//                                        }
//                                    }
                                    // TODO: Test this with worker thread dispatch.

                                    Map<Long, LocalResource> resourceMap = localResourceRepository.loadAndGetAllResources();
                                    LocalResource localResource = resourceMap.get(remoteLog.getResourceId());
                                    String partitionIODevicePath = localResourceRepository.getPartitionPath(localResource.getPartition());
                                    String resourceAbsolutePath =
                                            partitionIODevicePath + File.separator + localResource.getResourceName();
                                    localResource.setResourcePath(resourceAbsolutePath);
                                    index = (ILSMIndex) datasetLifecycleManager.get(resourceAbsolutePath);

                                    if (index == null) {
                                        localResourceMetadata = (ILocalResourceMetadata) localResource.getResourceObject();
                                        index = localResourceMetadata.createIndexInstance(txnSubSystem.getAsterixAppRuntimeContextProvider(),
                                                resourceAbsolutePath, localResource.getPartition(),
                                                localResourceRepository.getIODeviceNum(localResource.getPartition()));
                                        datasetLifecycleManager.register(resourceAbsolutePath, index);
                                        datasetLifecycleManager.open(resourceAbsolutePath);
                                        // must be closed for each job ?
                                    }

                                    ReplicationJob rJob = new ReplicationJob(remoteLog.getResourceId(), remoteLog
                                            .getJobId(), remoteLog.getDatasetId(), remoteLog.getPKHashValue(),
                                            remoteLog.getLogType(), remoteLog.getNewValue(), remoteLog.getNewOp(),
                                            txnSubSystem.getAsterixAppRuntimeContextProvider()
                                                    .getDatasetLifecycleManager());
                                    updateLocalOpTracker();
                                    materializationThreads.execute(rJob);
//                                    materializationThreads.execute(() -> {
//                                        try {
//                                            LOGGER.info("Waiting to write " + remoteLog.getLogRecordForDisplay());
//                                            Thread.sleep(10000);
//                                            materialize(remoteLog);
//                                            LOGGER.info("Wrote " + remoteLog.getLogRecordForDisplay());
//                                            updateLocalOpTracker();
//                                        } catch (Exception e) {
//                                            LOGGER.severe("Exception occurred while processing remoteLog");
//                                            e.printStackTrace();
//                                        }
//                                    });
//                                    materialize(remoteLog);
//                                    updateLocalOpTracker();
                                } catch (Exception e) {
                                    LOGGER.info("COULD NOT MATERIALIZE! : " + remoteLog.getLogRecordForDisplay());
                                    e.printStackTrace();
                                }
                            }
                        }
                        break;
                    case LogType.JOB_COMMIT:
                        commitCount++;
                        LOGGER.info("Received Job Commit for Job " + remoteLog.getJobId());
                        //updateLocalOpTracker(remoteLog.getJobId(), remoteLog.getPKHashValue(), remoteLog.getLogType
                        // ());

                    case LogType.ABORT:
                        abortCount++;
                        LogRecord jobTerminationLog = new LogRecord();
                        TransactionUtil.formJobTerminateLogRecord(jobTerminationLog, remoteLog.getJobId(),
                                remoteLog.getLogType() == LogType.JOB_COMMIT);
                        updateLocalOpTracker();
                        jobTerminationLog.setReplicationThread(this);
                        jobTerminationLog.setLogSource(LogSource.REMOTE);
                        logManager.log(jobTerminationLog);



                        //ITransactionContext txnCtx = txnSubSystem.getTransactionManager().getTransactionContext(new
                          //      JobId(remoteLog.getJobId()), false);
                        // Hot-Standby - abort associated transaction on replica?
                        // txnSubSystem.getTransactionManager()
                        // .abortTransaction
                        // (txnCtx, remoteLog
                        // .getDatasetId(),
                          //      remoteLog.getPKHashValue());
                        break;
                    case LogType.FLUSH:
                        flushCount++;
                        //store mapping information for flush logs to use them in incoming LSM components.
                        RemoteLogMapping flushLogMap = new RemoteLogMapping();
                        flushLogMap.setRemoteNodeID(remoteLog.getNodeId());
                        flushLogMap.setRemoteLSN(remoteLog.getLSN());
                        logManager.log(remoteLog);
                        //the log LSN value is updated by logManager.log(.) to a local value
                        flushLogMap.setLocalLSN(remoteLog.getLSN());
                        flushLogMap.numOfFlushedIndexes.set(remoteLog.getNumOfFlushedIndexes());
                        replicaUniqueLSN2RemoteMapping.put(flushLogMap.getNodeUniqueLSN(), flushLogMap);
                        synchronized (flushLogslock) {
                            flushLogslock.notify();
                        }
                        break;
                    default:
                        unknown++;
                        LOGGER.severe("Unsupported LogType: " + remoteLog.getLogType());
                }
            }

            LOGGER.info("Batch log processed counts: ");
            //int upCount, ecCount, upsertCount, commitCount, abortCount, flushCount, unknown;
            LOGGER.info("R_TYPE_COUNTS: UPDATE/ENTITY_COMMIT/UPSERT/COMMIT/ABORT/FLUSH/UNKOWN:" + upCount + " / " +
                    ecCount + " / " +  upsertCount + " / " + commitCount + " / " + abortCount + " / " + flushCount +
                    " / " + unknown);
        }

        /**
         * this method is called sequentially by LogPage (notifyReplicationTerminator)
         * for JOB_COMMIT and JOB_ABORT log types.
         */
        @Override
        public void notifyLogReplicationRequester(LogRecord logRecord) {
            pendingNotificationRemoteLogsQ.offer(logRecord);
        }

        @Override
        public SocketChannel getReplicationClientSocket() {
            return socketChannel;
        }
    }

    /**
     * This thread is responsible for sending JOB_COMMIT/ABORT ACKs to replication clients.
     */
    private class ReplicationNotifier extends Thread {
        @Override
        public void run() {
            Thread.currentThread().setName("ReplicationNotifier Thread");
            while (true) {
                try {
                    LogRecord logRecord = pendingNotificationRemoteLogsQ.take();
                    //send ACK to requester
                    logRecord.getReplicationThread().getReplicationClientSocket().socket().getOutputStream()
                            .write((localNodeID + ReplicationProtocol.JOB_REPLICATION_ACK + logRecord.getJobId()
                                    + System.lineSeparator()).getBytes());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.log(Level.WARNING, "Failed to send job replication ACK", e);
                    }
                }
            }
        }
    }

    /**
     * This thread is responsible for synchronizing the LSN of
     * the received LSM components to a local LSN.
     */
    private class LSMComponentsSyncService extends Thread {
        private static final int BULKLOAD_LSN = 0;

        @Override
        public void run() {
            Thread.currentThread().setName("LSMComponentsSyncService Thread");

            while (true) {
                try {
                    LSMComponentLSNSyncTask syncTask = lsmComponentRemoteLSN2LocalLSNMappingTaskQ.take();
                    LSMComponentProperties lsmCompProp = lsmComponentId2PropertiesMap.get(syncTask.getComponentId());
                    syncLSMComponentFlushLSN(lsmCompProp, syncTask);
                    updateLSMComponentRemainingFiles(lsmCompProp.getComponentId());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    if (LOGGER.isLoggable(Level.SEVERE)) {
                        LOGGER.log(Level.SEVERE, "Unexpected exception during LSN synchronization", e);
                    }
                }

            }
        }

        private void syncLSMComponentFlushLSN(LSMComponentProperties lsmCompProp, LSMComponentLSNSyncTask syncTask)
                throws InterruptedException, IOException {
            long remoteLSN = lsmCompProp.getOriginalLSN();
            //LSN=0 (bulkload) does not need to be updated and there is no flush log corresponding to it
            if (remoteLSN == BULKLOAD_LSN) {
                //since this is the first LSM component of this index,
                //then set the mapping in the LSN_MAP to the current log LSN because
                //no other log could've been received for this index since bulkload replication is synchronous.
                lsmCompProp.setReplicaLSN(logManager.getAppendLSN());
                return;
            }

            //path to the LSM component file
            Path path = Paths.get(syncTask.getComponentFilePath());
            if (lsmCompProp.getReplicaLSN() == null) {
                if (lsmCompProp.getOpType() == LSMOperationType.FLUSH) {
                    //need to look up LSN mapping from memory
                    RemoteLogMapping remoteLogMap = replicaUniqueLSN2RemoteMapping.get(lsmCompProp.getNodeUniqueLSN());
                    //wait until flush log arrives, and verify the LSM component file still exists
                    //The component file could be deleted if its NC fails.
                    while (remoteLogMap == null && Files.exists(path)) {
                        synchronized (flushLogslock) {
                            flushLogslock.wait();
                        }
                        remoteLogMap = replicaUniqueLSN2RemoteMapping.get(lsmCompProp.getNodeUniqueLSN());
                    }

                    /**
                     * file has been deleted due to its remote primary replica failure
                     * before its LSN could've been synchronized.
                     */
                    if (remoteLogMap == null) {
                        return;
                    }
                    lsmCompProp.setReplicaLSN(remoteLogMap.getLocalLSN());
                } else if (lsmCompProp.getOpType() == LSMOperationType.MERGE) {
                    //need to load the LSN mapping from disk
                    Map<Long, Long> lsmMap = replicaResourcesManager
                            .getReplicaIndexLSNMap(lsmCompProp.getReplicaComponentPath(replicaResourcesManager));
                    Long mappingLSN = lsmMap.get(lsmCompProp.getOriginalLSN());
                    if (mappingLSN == null) {
                        /**
                         * this shouldn't happen unless this node just recovered and
                         * the first component it received is a merged component due
                         * to an on-going merge operation while recovery on the remote
                         * replica. In this case, we use the current append LSN since
                         * no new records exist for this index, otherwise they would've
                         * been flushed. This could be prevented by waiting for any IO
                         * to finish on the remote replica during recovery.
                         */
                        mappingLSN = logManager.getAppendLSN();
                    }
                    lsmCompProp.setReplicaLSN(mappingLSN);
                }
            }

            if (Files.notExists(path)) {
                /**
                 * This could happen when a merged component arrives and deletes
                 * the flushed component (which we are trying to update) before
                 * its flush log arrives since logs and components are received
                 * on different threads.
                 */
                return;
            }

            File destFile = new File(syncTask.getComponentFilePath());
            //prepare local LSN buffer
            ByteBuffer metadataBuffer = ByteBuffer.allocate(Long.BYTES);
            metadataBuffer.putLong(lsmCompProp.getReplicaLSN());
            metadataBuffer.flip();

            //replace the remote LSN value by the local one
            try (RandomAccessFile fileOutputStream = new RandomAccessFile(destFile, "rw");
                    FileChannel fileChannel = fileOutputStream.getChannel()) {
                long lsnStartOffset = syncTask.getLSNByteOffset();
                while (metadataBuffer.hasRemaining()) {
                    lsnStartOffset += fileChannel.write(metadataBuffer, lsnStartOffset);
                }
                fileChannel.force(true);
            }
        }
    }

    private class OpTrackerProfiler extends Thread {
        private long interval = 5000;
        public OpTrackerProfiler(long interval) {
            this.interval = interval;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(interval);
                    LOGGER.info(printOpStatus());
                } catch (InterruptedException e) {
                    LOGGER.severe("Unexepected error during profiling");
                    e.printStackTrace();
                }
            }
        }
    }
}
