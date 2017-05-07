package org.apache.asterix.replication.management;

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.context.DatasetInfo;
import org.apache.asterix.common.context.DatasetLifecycleManager;
import org.apache.asterix.common.context.IndexInfo;
import org.apache.asterix.common.ioopcallbacks.AbstractLSMIOOperationCallback;
import org.apache.asterix.common.replication.IReplicaResourcesManager;
import org.apache.asterix.common.transactions.*;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.common.file.ILocalResourceRepository;
import org.apache.hyracks.storage.common.file.LocalResource;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by msa on 3/24/17.
 */
public class StreamingReplicationThread implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(StreamingReplicationManager.class.getName());

    private final ILocalResourceRepository localResourceRepository;
    private final IDatasetLifecycleManager datasetLifecycleManager;
    private final ITransactionSubsystem txnSubSystem;
    private IReplicaResourcesManager replicaResourcesManager;

    private Map<Long, LocalResource> resourceMap;
    private Set<Integer> nodePartitions;
    private static final Object resourceMapLock = new Object();
    private final Map<Integer, LogReplayThread> partitionReplayThreadMap;

    public StreamingReplicationThread(IAppRuntimeContextProvider appRuntimeContextProvider,
            IReplicaResourcesManager replicaResourcesManager) {
        this.replicaResourcesManager = replicaResourcesManager;

        this.txnSubSystem = appRuntimeContextProvider.getTransactionSubsystem();
        this.localResourceRepository = txnSubSystem.getAsterixAppRuntimeContextProvider().getLocalResourceRepository();
        this.nodePartitions = ((PersistentLocalResourceRepository) localResourceRepository).getInactivePartitions();
        this.datasetLifecycleManager = appRuntimeContextProvider.getDatasetLifecycleManager();
        this.partitionReplayThreadMap = new HashMap<>();
        LOGGER.log(Level.INFO, "REPL: Streaming replication initialized!");
        try {
            refreshLocalResourceMap();
        } catch (HyracksDataException e) {
            e.printStackTrace();
        }
        startReplayThreads();
    }

    @Override public void run() {
        Thread.currentThread().setName("Streaming Replication Thread");
    }

    public void flushAllWriteQs() {
        LOGGER.info("Requesting threads to flush their incoming write buffers");
        partitionReplayThreadMap.values().stream().forEach(LogReplayThread::requestIncomingBufferFlush);
    }

    private void startReplayThreads() {
        ExecutorService service = Executors.newFixedThreadPool(nodePartitions.size());
        for (int partition : nodePartitions) {
            partitionReplayThreadMap.put(partition, new LogReplayThread(partition));
        }
        partitionReplayThreadMap.values().forEach(service::submit);
    }

    public void submit(ILogRecord logRecord) throws InterruptedException {
        switch (logRecord.getLogType()) {
            case LogType.UPDATE:
                partitionReplayThreadMap.get(logRecord.getResourcePartition()).submit(logRecord);
                break;
            case LogType.FLUSH:
                for (LogReplayThread replayThread : partitionReplayThreadMap.values()) {
                    replayThread.submit(logRecord);
                }
                break;
            case LogType.ENTITY_COMMIT:
            case LogType.UPSERT_ENTITY_COMMIT:
                break;
            default:
                LOGGER.severe("Unsupported log type for replay! ");
        }
    }

    public void refreshLocalResourceMap() throws HyracksDataException {
        this.resourceMap = ((PersistentLocalResourceRepository) localResourceRepository).loadAndGetAllResources();
    }

    /**
     * This class is responsible for replaying incoming remote log records for a specific resource partition
     */
    private class LogReplayThread extends Thread {

        private final int partition;
        private final int numPages;
        private final int pageSize;

        private final BlockingQueue<ByteBuffer> jobQ;
        private final BlockingQueue<ByteBuffer> emptyQ;
        private final ILogRecord logRecord;

        private ByteBuffer incomingRemoteLogBufferPage;
        private ByteBuffer remoteLogBufferPage;

        public LogReplayThread(int partition) {
            this.partition = partition;
            this.jobQ = new LinkedBlockingQueue<>();
            this.emptyQ = new LinkedBlockingDeque<>(txnSubSystem.getLogManager().getNumLogPages());
            this.numPages = txnSubSystem.getLogManager().getNumLogPages();
            this.pageSize = txnSubSystem.getLogManager().getLogPageSize();
            this.logRecord = new LogRecord();
            initialize();
        }

        private void initialize() {
            IntStream.range(0, numPages).forEach(page -> emptyQ.offer(ByteBuffer.allocate(pageSize)));
            this.incomingRemoteLogBufferPage = ByteBuffer.allocate(pageSize);
            remoteLogBufferPage = null;
        }

        public void submit(ILogRecord logRecord) throws InterruptedException {
            synchronized (this) {
                if (incomingRemoteLogBufferPage.remaining() < logRecord.getLogSize()) {
                    incomingRemoteLogBufferPage.flip();
                    jobQ.offer(incomingRemoteLogBufferPage);
                    this.notify();
                    incomingRemoteLogBufferPage = emptyQ.take();
                    incomingRemoteLogBufferPage.clear();
                }
                LOGGER.info("Copying remote log buffer on " + partition + " PK " + logRecord.getPKHashValue());
                logRecord.writeRemoteLogRecord(incomingRemoteLogBufferPage);
                this.notify();
            }
        }

        public void requestIncomingBufferFlush() {
            synchronized (this) {
                try {
                    flushIncoming();
                } catch (InterruptedException e) {
                    LOGGER.severe("Interrupted while trying to issue a flush request");
                }
            }
        }

        private void flushIncoming() throws InterruptedException {
            incomingRemoteLogBufferPage.flip();
            jobQ.offer(incomingRemoteLogBufferPage);
            incomingRemoteLogBufferPage = emptyQ.take();
            incomingRemoteLogBufferPage.clear();
        }

        @Override public void run() {
            String name = "RMT-" + partition;
            Thread.currentThread().setName(name);
            while (true) {
                try {
                    synchronized (this) {
                        while (jobQ.isEmpty()) {
                            if (incomingRemoteLogBufferPage.position() == 0) {
                                this.wait();
                            } else {
                                try {
                                    LOGGER.info("Stealing current buffer into replay thread");
                                    flushIncoming();
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                                break;
                            }
                        }
                    }
                    remoteLogBufferPage = jobQ.take();
                    int counter = 0;
                    Instant start = Instant.now();
                    while (remoteLogBufferPage.hasRemaining()) {
                        logRecord.readRemoteLog(remoteLogBufferPage);
                        counter++;
                        LOGGER.log(Level.INFO,
                                "REPL: " + name + " : read log record " + logRecord.getLogRecordForDisplay());
                        try {
                            materialize();
                        } catch (Exception e) {
                            LOGGER.log(Level.INFO, "REPL FAILED: " + name + " " + logRecord.getLogRecordForDisplay());
                        }
                    }
                    Instant end = Instant.now();
                    LOGGER.log(Level.INFO,
                            "REPL: STATS: " + name + " Num Logs in prev remoteLogBufferPage: " + counter + " JOBQ "
                                    + "has" + jobQ.size() + " buffers waiting! and time to complete " + Duration
                                    .between(start, end).toMillis());
                    remoteLogBufferPage.clear();
                    emptyQ.offer(remoteLogBufferPage);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        private void materialize() {
            long resourceId = logRecord.getResourceId();
            if (logRecord.getLogType() == LogType.UPDATE) {
                try {
                    LocalResource localResource = resourceMap.get(resourceId);
                    if (localResource == null) {
                        synchronized (resourceMapLock) {
                            localResource = resourceMap.get(resourceId);
                            if (localResource == null) {
                                LOGGER.log(Level.INFO,
                                        "Local resource " + resourceId + " not found!, refreshing the local "
                                                + "resource repository.");
                                refreshLocalResourceMap();
                                localResource = resourceMap.get(resourceId);
                            }
                        }
                    }
                    Resource localResourceMetadata = (Resource) localResource.getResource();
                    ILSMIndex index = (ILSMIndex) datasetLifecycleManager.get(localResource.getPath());
                    if (index == null) {
                        index = localResourceMetadata
                                .createIndexInstance(txnSubSystem.getServiceContext(), localResource);
                        ((DatasetLifecycleManager) datasetLifecycleManager)
                                .registerInactivePartitionIndex(localResource.getPath(), index);
                        datasetLifecycleManager.open(localResource.getPath());
                    }

                    ILSMIndexAccessor indexAccessor = index
                            .createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);

                    if (logRecord.getNewOp() == IndexOperation.INSERT.ordinal()) {
                        indexAccessor.forceInsert(logRecord.getNewValue()); // TODO: Changed from forceInsert to insert.
                    } else if (logRecord.getNewOp() == IndexOperation.DELETE.ordinal()) {
                        indexAccessor.forceDelete(logRecord.getNewValue()); // TODO: Changed from forceDelete to delete.
                    } else {
                        LOGGER.log(Level.SEVERE, "Unknown Optype to replicate");
                    }
                } catch (HyracksDataException e) {
                    LOGGER.log(Level.SEVERE, "Replicating a record failed");
                } catch (IndexException e) {
                    // TODO: change this to a catch all exception?
                    LOGGER.severe("Could not replay remote index modification");
                }
            } else if (logRecord.getLogType() == LogType.FLUSH) {
                try {
                    flushDataset(logRecord, replicaResourcesManager.getRemoteNodePartitions(logRecord.getNodeId()));
                } catch (InterruptedException e) {
                    // Handle this case
                    e.printStackTrace();
                } catch (HyracksDataException e) {
                    // Handle this case
                    e.printStackTrace();
                }
            }
        }

        public void flushDataset(ILogRecord logRecord, Set<Integer> remoteNodePartitions)
                throws InterruptedException, HyracksDataException {
            String sourceNode = logRecord.getNodeId();
            int datasetId = logRecord.getDatasetId();
            DatasetInfo dsInfo = datasetLifecycleManager.getDatasetInfo(datasetId);
            List<IndexInfo> inactiveIndexes = dsInfo.getReplicaParitionIndexList();
            inactiveIndexes = inactiveIndexes.stream()
                    .filter(index -> remoteNodePartitions.contains(index.getPartitionId()))
                    .collect(Collectors.toList());
            LOGGER.info("Indexes to schedule a flush: " + inactiveIndexes);

            // Get all the inactive partitions to have the same state as the primary by flushing its job queues.
            for (IndexInfo indexInfo : inactiveIndexes) {
                if (indexInfo.getPartitionId() == partition) {
                    ILSMIndexAccessor accessor = indexInfo.getIndex()
                            .createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);

                    AbstractLSMIOOperationCallback ioOpCallback = (AbstractLSMIOOperationCallback) indexInfo.getIndex()
                            .getIOOperationCallback();
                    long localAppendLSN = txnSubSystem.getLogManager().getAppendLSN();
                    LOGGER.info("Setting local LSN of the flushed index " + indexInfo + " to " + localAppendLSN);
                    ioOpCallback.updateLastLSN(localAppendLSN);
                    accessor.scheduleFlush(indexInfo.getIndex().getIOOperationCallback());
                }
            }
            LOGGER.info("Flushing complete!");
        }
    }
}