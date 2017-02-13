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
import org.apache.asterix.common.config.MetadataProperties;
import org.apache.asterix.common.transactions.IAppRuntimeContextProvider;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.config.IPropertiesProvider;
import org.apache.asterix.common.config.ReplicationProperties;
import org.apache.asterix.common.replication.IReplicaResourcesManager;
import org.apache.asterix.common.replication.IReplicationChannel;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.transactions.*;
import org.apache.asterix.replication.functions.ReplicationProtocol;
import org.apache.asterix.replication.logging.RemoteLogMapping;
import org.apache.asterix.replication.storage.LSMComponentLSNSyncTask;
import org.apache.asterix.replication.storage.LSMComponentProperties;
import org.apache.asterix.replication.storage.ReplicaResourcesManager;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.hyracks.api.application.INCApplicationContext;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.util.StorageUtil;
import org.apache.hyracks.util.StorageUtil.StorageUnit;

/**
 * This class is used to receive and process replication requests from remote replicas or replica events from CC
 */
public class ReplicationChannel extends Thread implements IReplicationChannel {

    private static final Logger LOGGER = Logger.getLogger(ReplicationChannel.class.getName());
    private final ExecutorService replicationThreads;
    private final String localNodeID;
    protected final ILogManager logManager;
    protected final ReplicaResourcesManager replicaResourcesManager;
    private SocketChannel socketChannel = null;
    private ServerSocketChannel serverSocketChannel = null;
    protected final IReplicationManager replicationManager;
    protected final ReplicationProperties replicationProperties;
    protected final IAppRuntimeContextProvider appContextProvider;
    protected final LinkedBlockingQueue<LSMComponentLSNSyncTask> lsmComponentRemoteLSN2LocalLSNMappingTaskQ;
    protected final LinkedBlockingQueue<LogRecord> pendingNotificationRemoteLogsQ;
    protected final Map<String, LSMComponentProperties> lsmComponentId2PropertiesMap;
    protected final Map<String, RemoteLogMapping> replicaUniqueLSN2RemoteMapping;
    private final LSMComponentsSyncService lsmComponentLSNMappingService;
    protected final Set<Integer> nodeHostedPartitions;
    private final ReplicationNotifier replicationNotifier;
    protected final Object flushLogslock = new Object();

    protected final ITransactionSubsystem txnSubSystem;
    private final ExecutorService materializationThreads;

    protected final StreamingReplicationManager streamingReplicationManager;
    private final boolean isStreaming = true;

    // TODO: Change to (RID, PKHashValue, lastOpLSN)
    public Map<Long, Map<Integer, Long>> inflightOps;
    private OpTrackerProfiler profiler;
    private Map<Integer, Integer> PKstatus; // Why?
    private int opCounter = 0;

    // TODO: On merge, check if localResourceRepository (added by msa) can be used instead of the
    // localResourceRep introduced by Change-Id: I1d1012f5541ce786f127866efefb9f3db434fedd
    protected final IDatasetLifecycleManager dsLifecycleManager;
    protected final PersistentLocalResourceRepository localResourceRep;
    protected final MetadataProperties metadataProperties;


    public ReplicationChannel(String nodeId, ReplicationProperties replicationProperties, ILogManager logManager,
            IReplicaResourcesManager replicaResoucesManager, IReplicationManager replicationManager,
            INCApplicationContext appContext, IAppRuntimeContextProvider asterixAppRuntimeContextProvider,
            MetadataProperties metadataProperties) {
        this.logManager = logManager;
        this.localNodeID = nodeId;
        this.replicaResourcesManager = (ReplicaResourcesManager) replicaResoucesManager;
        this.replicationManager = replicationManager;
        this.replicationProperties = replicationProperties;
        this.appContextProvider = asterixAppRuntimeContextProvider;
        this.dsLifecycleManager = asterixAppRuntimeContextProvider.getDatasetLifecycleManager();
        this.localResourceRep = (PersistentLocalResourceRepository) asterixAppRuntimeContextProvider
                .getLocalResourceRepository();
        lsmComponentRemoteLSN2LocalLSNMappingTaskQ = new LinkedBlockingQueue<>();
        pendingNotificationRemoteLogsQ = new LinkedBlockingQueue<>();
        lsmComponentId2PropertiesMap = new ConcurrentHashMap<>();
        replicaUniqueLSN2RemoteMapping = new ConcurrentHashMap<>();
        lsmComponentLSNMappingService = new LSMComponentsSyncService();
        replicationNotifier = new ReplicationNotifier();
        replicationThreads = Executors.newCachedThreadPool(appContext.getThreadFactory());
        Map<String, ClusterPartition[]> nodePartitions = ((IPropertiesProvider) asterixAppRuntimeContextProvider
                .getAppContext()).getMetadataProperties().getNodePartitions();
        Set<String> nodeReplicationClients = replicationProperties.getRemotePrimaryReplicasIds(nodeId);
        List<Integer> clientsPartitions = new ArrayList<>();
        for (String clientId : nodeReplicationClients) {
            for (ClusterPartition clusterPartition : nodePartitions.get(clientId)) {
                clientsPartitions.add(clusterPartition.getPartitionId());
            }
        }
        nodeHostedPartitions = new HashSet<>(clientsPartitions.size());
        nodeHostedPartitions.addAll(clientsPartitions);
        txnSubSystem = logManager.getTransactionSubsystem();


        inflightOps = new HashMap<>();
        PKstatus = new HashMap<>();
        LOGGER.log(Level.INFO,"Starting Hot-Standby OpTracker Thread");
        this.profiler = new OpTrackerProfiler(60000);
        this.profiler.start();
        materializationThreads = Executors.newCachedThreadPool(appContext.getThreadFactory());
        this.metadataProperties = metadataProperties;
        this.streamingReplicationManager = new StreamingReplicationManager(txnSubSystem,
                asterixAppRuntimeContextProvider, metadataProperties);
        LOGGER.log(Level.INFO, "REPL: Initialized replicationchannel thread!");

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
            InetSocketAddress replicationChannelAddress = new InetSocketAddress(InetAddress.getByName(nodeIP),
                    dataPort);
            serverSocketChannel.socket().bind(replicationChannelAddress);
            lsmComponentLSNMappingService.start();
            replicationNotifier.start();
            LOGGER.log(Level.INFO, "opened Replication Channel @ IP Address: " + nodeIP + ":" + dataPort);

            //start accepting replication requests
            while (true) {
                socketChannel = serverSocketChannel.accept();
                socketChannel.configureBlocking(true);
                //start a new thread to handle the request
                LOGGER.log(Level.INFO, "New replication thread requested, creating new replication thread");
                if (isStreaming) {
                    replicationThreads.execute(new StreamingReplicationThread(this, socketChannel));
                } else {
                    replicationThreads.execute(new PassiveReplicationThread(this, socketChannel));
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Could not open replication channel @ IP Address: " + nodeIP + ":" + dataPort, e);
        }
    }

    protected void updateLSMComponentRemainingFiles(String lsmComponentId) throws IOException {
        LSMComponentProperties lsmCompProp = lsmComponentId2PropertiesMap.get(lsmComponentId);
        int remainingFile = lsmCompProp.markFileComplete();

        //clean up when all the LSM component files have been received.
        if (remainingFile == 0) {
            if (lsmCompProp.getOpType() == LSMOperationType.FLUSH && lsmCompProp.getReplicaLSN() != null
                    && replicaUniqueLSN2RemoteMapping.containsKey(lsmCompProp.getNodeUniqueLSN())) {
                int remainingIndexes = replicaUniqueLSN2RemoteMapping
                        .get(lsmCompProp.getNodeUniqueLSN()).numOfFlushedIndexes.decrementAndGet();
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
