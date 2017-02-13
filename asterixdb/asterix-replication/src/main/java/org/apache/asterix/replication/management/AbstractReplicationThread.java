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

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.config.IPropertiesProvider;
import org.apache.asterix.common.context.IndexInfo;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.ioopcallbacks.AbstractLSMIOOperationCallback;
import org.apache.asterix.common.replication.IReplicationStrategy;
import org.apache.asterix.common.replication.IReplicationThread;
import org.apache.asterix.common.replication.ReplicaEvent;
import org.apache.asterix.common.storage.IndexFileProperties;
import org.apache.asterix.common.transactions.*;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.replication.functions.ReplicaFilesRequest;
import org.apache.asterix.replication.functions.ReplicaIndexFlushRequest;
import org.apache.asterix.replication.functions.ReplicationProtocol;
import org.apache.asterix.replication.storage.LSMComponentLSNSyncTask;
import org.apache.asterix.replication.storage.LSMComponentProperties;
import org.apache.asterix.replication.storage.LSMIndexFileProperties;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.asterix.transaction.management.service.recovery.RecoveryManager;
import org.apache.asterix.transaction.management.service.transaction.TransactionManagementConstants;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.common.file.LocalResource;
import org.apache.hyracks.util.StorageUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.logging.Logger;

/**
 * A replication thread is created per received replication request.
 */
public abstract class AbstractReplicationThread implements IReplicationThread {
    protected ReplicationChannel replicationChannel;
    protected final SocketChannel socketChannel;
    //private final LogRecord remoteLog;
    protected LogRecord remoteLog;
    private IDatasetLifecycleManager datasetLifecycleManager;
    private static final Logger LOGGER = Logger.getLogger(AbstractReplicationThread.class.getName());
    private final PersistentLocalResourceRepository localResourceRepository;
    protected static final int INTIAL_BUFFER_SIZE = StorageUtil.getSizeInBytes(4, StorageUtil.StorageUnit.KILOBYTE);
    protected static final int LOG_REPLICATION_END_HANKSHAKE_LOG_SIZE = 1;
    protected ByteBuffer inBuffer;
    protected ByteBuffer outBuffer;

    public AbstractReplicationThread(ReplicationChannel replicationChannel, SocketChannel socketChannel) {
        this.replicationChannel = replicationChannel;
        this.socketChannel = socketChannel;
        remoteLog = new LogRecord();
        datasetLifecycleManager = replicationChannel.appContextProvider.getDatasetLifecycleManager();
        localResourceRepository = (PersistentLocalResourceRepository) replicationChannel.txnSubSystem
                .getAsterixAppRuntimeContextProvider().getLocalResourceRepository();
        inBuffer = ByteBuffer.allocate(INTIAL_BUFFER_SIZE);
        outBuffer = ByteBuffer.allocate(INTIAL_BUFFER_SIZE);
    }

    private synchronized void updateLocalOpTracker() {
        long resourceId = remoteLog.getDatasetId();
        int PKHashValue = remoteLog.getPKHashValue();
        long lastLSN = remoteLog.getLSN();


        Map<Integer, Long> entityLocks = null;
        if (!replicationChannel.inflightOps.containsKey(resourceId)) {
            entityLocks = new HashMap<>();
            replicationChannel.inflightOps.put(resourceId, entityLocks);
        } else {
            entityLocks = replicationChannel.inflightOps.get(resourceId);
        }
        if (entityLocks.containsKey(PKHashValue)) {
            if (remoteLog.getLogType() == LogType.ENTITY_COMMIT || remoteLog.getLogType() == LogType.JOB_COMMIT) {
                LOGGER.info("Current last LSN before entity commit for PK: " + PKHashValue + ", LSN: " + entityLocks
                        .get(PKHashValue) + " logtype: " + remoteLog.getLogType());
                LOGGER.info("Received EC for RID: " + resourceId + " PK: " + PKHashValue + "with LSN: " +
                        entityLocks.get(PKHashValue));
                long removedLSN = entityLocks.remove(PKHashValue);
                LOGGER.info("Removing PKHash, returned: " + removedLSN);
                LOGGER.info(replicationChannel.printOpStatus());
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

        IAppRuntimeContextProvider appRuntimeContext =
                replicationChannel.txnSubSystem.getAsterixAppRuntimeContextProvider();
        IDatasetLifecycleManager datasetLifecycleManager = appRuntimeContext.getDatasetLifecycleManager();

        ILSMIndex index = null;
        Resource localResourceMetadata = null;

        long resourceId = remoteLog.getResourceId();
        Map<Long, LocalResource> resourceMap = localResourceRepository.loadAndGetAllResources();
        LocalResource localResource = resourceMap.get(resourceId);
        ITransactionContext txnCtx = null;
        try {
            txnCtx = replicationChannel.txnSubSystem.getTransactionManager().getTransactionContext(new JobId(remoteLog
                    .getJobId()), true);
        } catch (ACIDException e) {
            e.printStackTrace();
            LOGGER.severe("REPL: TXN CONTEXT NOT FOUND!!!");
        }

        replicationChannel.txnSubSystem.getLockManager().lock(new DatasetId(remoteLog.getDatasetId()), remoteLog.getPKHashValue(),
                TransactionManagementConstants.LockManagerConstants.LockMode.X, txnCtx);

        //JobId jobID = org.apache.asterix.transaction.management.service.transaction.JobIdFactory.generateJobId();

        if (localResource == null) {
            throw new HyracksDataException("Local resource not found!");
        }

        // TODO: Can this be created at a different time?
        // TODO: Log this information. Can this operation be done at a different time?
        localResourceMetadata = (Resource) localResource.getResource();
        index = (ILSMIndex) datasetLifecycleManager.get(localResource.getPath());
        if (index == null) {
            index = localResourceMetadata.createIndexInstance(appRuntimeContext, localResource);
            datasetLifecycleManager.register(localResource.getPath(), index);
            datasetLifecycleManager.open(localResource.getPath());
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
        //datasetLifecycleManager.close(localResource.getPath());

        replicationChannel.txnSubSystem.getLockManager().unlock(new DatasetId(remoteLog.getDatasetId()), remoteLog.getPKHashValue(),
                TransactionManagementConstants.LockManagerConstants.LockMode.X, txnCtx);
        //verify(remoteLog);
    }

    /**
     * this method is called sequentially by LogPage (notifyReplicationTerminator)
     * for JOB_COMMIT and JOB_ABORT log types.
     */
    @Override
    public void notifyLogReplicationRequester(LogRecord logRecord) {
        replicationChannel.pendingNotificationRemoteLogsQ.offer(logRecord);
    }

    @Override
    public SocketChannel getReplicationClientSocket() {
        return socketChannel;
    }

    protected void handleLSMComponentProperties() throws IOException {
        inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
        LSMComponentProperties lsmCompProp = ReplicationProtocol.readLSMPropertiesRequest(inBuffer);
        //create mask to indicate that this component is not valid yet
        replicationChannel.replicaResourcesManager.createRemoteLSMComponentMask(lsmCompProp);
        replicationChannel.lsmComponentId2PropertiesMap.put(lsmCompProp.getComponentId(), lsmCompProp);
    }

    protected void handleReplicateFile() throws IOException {
        inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
        LSMIndexFileProperties afp = ReplicationProtocol.readFileReplicationRequest(inBuffer);

        //get index path
        String indexPath = replicationChannel.replicaResourcesManager.getIndexPath(afp);
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
                    replicationChannel.updateLSMComponentRemainingFiles(componentId);
                }
            } else {
                //index metadata file
                replicationChannel.replicaResourcesManager.initializeReplicaIndexLSNMap(indexPath, replicationChannel.logManager.getAppendLSN());
            }
        }
    }

    protected void handleGetReplicaMaxLSN() throws IOException {
        long maxLNS = replicationChannel.logManager.getAppendLSN();
        outBuffer.clear();
        outBuffer.putLong(maxLNS);
        outBuffer.flip();
        NetworkingUtil.transferBufferToChannel(socketChannel, outBuffer);
    }

    protected void handleGetReplicaFiles() throws IOException {
        inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
        ReplicaFilesRequest request = ReplicationProtocol.readReplicaFileRequest(inBuffer);

        LSMIndexFileProperties fileProperties = new LSMIndexFileProperties();

        List<String> filesList;
        Set<Integer> partitionIds = request.getPartitionIds();
        Set<String> requesterExistingFiles = request.getExistingFiles();
        Map<Integer, ClusterPartition> clusterPartitions = ((IPropertiesProvider) replicationChannel.appContextProvider
                .getAppContext()).getMetadataProperties().getClusterPartitions();

        final IReplicationStrategy repStrategy = replicationChannel.replicationProperties.getReplicationStrategy();
        // Flush replicated datasets to generate the latest LSM components
        replicationChannel.dsLifecycleManager.flushDataset(repStrategy);
        for (Integer partitionId : partitionIds) {
            ClusterPartition partition = clusterPartitions.get(partitionId);
            filesList = replicationChannel.replicaResourcesManager.getPartitionIndexesFiles(partition.getPartitionId(), false);
            //start sending files
            for (String filePath : filesList) {
                // Send only files of datasets that are replciated.
                IndexFileProperties indexFileRef = replicationChannel.localResourceRep.getIndexFileRef(filePath);
                if (!repStrategy.isMatch(indexFileRef.getDatasetId())) {
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

    protected void handleLogReplication() throws IOException, ACIDException {
        //set initial buffer size to a log buffer page size
        inBuffer = ByteBuffer.allocate(replicationChannel.logManager.getLogPageSize());
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

    protected void handleReplicaEvent() throws IOException {
        inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
        ReplicaEvent event = ReplicationProtocol.readReplicaEventRequest(inBuffer);
        replicationChannel.replicationManager.reportReplicaEvent(event);
    }

    protected void handleDeleteFile() throws IOException {
        inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
        LSMIndexFileProperties fileProp = ReplicationProtocol.readFileReplicationRequest(inBuffer);
        replicationChannel.replicaResourcesManager.deleteIndexFile(fileProp);
        if (fileProp.requiresAck()) {
            ReplicationProtocol.sendAck(socketChannel);
        }
    }

    public abstract void processLogsBatch(ByteBuffer inBuffer) throws ACIDException;

    protected void handleFlushIndex() throws IOException {
        inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
        //read which indexes are requested to be flushed from remote replica
        ReplicaIndexFlushRequest request = ReplicationProtocol.readReplicaIndexFlushRequest(inBuffer);
        Set<Long> requestedIndexesToBeFlushed = request.getLaggingRescouresIds();

        /**
         * check which indexes can be flushed (open indexes) and which cannot be
         * flushed (closed or have empty memory component).
         */
        IDatasetLifecycleManager datasetLifeCycleManager = replicationChannel.appContextProvider.getDatasetLifecycleManager();
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
}
