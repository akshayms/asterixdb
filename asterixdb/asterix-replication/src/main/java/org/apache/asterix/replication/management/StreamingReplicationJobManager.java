package org.apache.asterix.replication.management;

import org.apache.asterix.common.api.IAppRuntimeContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.config.MetadataProperties;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.replication.IReplicationChannel;
import org.apache.asterix.common.replication.IReplicationThread;
import org.apache.asterix.common.transactions.*;
import org.apache.asterix.replication.functions.ReplicationProtocol;
import org.apache.asterix.transaction.management.resource.LSMBTreeLocalResourceMetadata;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.common.file.ILocalResourceRepository;
import org.apache.hyracks.storage.common.file.LocalResource;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by msa on 3/24/17.
 */
public class StreamingReplicationJobManager implements IReplicationThread {

    private static final Logger LOGGER = Logger.getLogger(StreamingReplicationManager.class.getName());
    private final Map<Integer, BlockingQueue<ByteBuffer>> partitionReplicationQs;
    private final Map<Integer, ByteBuffer> partitionWriteBuffer;
    // TODO: Change to a single bytebuffer list per partition.
    private final BlockingQueue<ByteBuffer> freeBufferQ;
    private final ITransactionSubsystem txnSubSystem;
    private Set<Integer> nodePartitions;
    private int numPartitions;
    private boolean replicationEnabled = true;
    private List<Object> partitionMonitors;

    private static final int DEFAULT_SIZE = 20;
    private static final int DEFAULT_LOG_PAGE_SIZE = 15000;
    private static final int MAX_QUEUE_LENGTH = 15;
    private Object monitor = new Object();
    private final int ACTIVE_LOG_INDEX = 0;
    private final int INACTIVE_LOG_INDEX = 1;

    private final ILocalResourceRepository localResourceRepository;
    private final IDatasetLifecycleManager datasetLifecycleManager;
    //private final List<AtomicInteger> counters;
    private final Object bufferFlipMonitor = new Object();

    private Map<Long, LocalResource> resourceMap;
    private Map<Integer, Set<Integer>> jobResourcesMap;
    private static final Object resourceMapLock = new Object();

    //public StreamingReplicationManager(ITransactionSubsystem txnSubSystem, IAppRuntimeContextProvider
    //asterixAppRuntimeContextProvider, MetadataProperties metadataProperties)
    public StreamingReplicationJobManager(IAppRuntimeContextProvider appRuntimeContextProvider) {
    //public StreamingReplicationJobManager(IReplicationChannel replicationChannel, SocketChannel socketChannel) {
        this.partitionReplicationQs = new ConcurrentHashMap<>();
        this.txnSubSystem = appRuntimeContextProvider.getTransactionSubsystem();
        this.nodePartitions = appRuntimeContextProvider.getAppContext().getMetadataProperties().getClusterPartitions()
                .keySet();
        this.numPartitions = nodePartitions.size();
        this.partitionWriteBuffer = new ConcurrentHashMap<>();
        this.partitionMonitors = new ArrayList<>();
        this.freeBufferQ = new LinkedBlockingQueue<>();
        this.localResourceRepository = txnSubSystem
                .getAsterixAppRuntimeContextProvider().getLocalResourceRepository();
        //this.nodePartitions = ((PersistentLocalResourceRepository) localResourceRepository).getInactivePartitions();
        nodePartitions.stream().forEach(partitionId -> {
            // 3 buffers to start out with per partition on average.
            freeBufferQ.offer(ByteBuffer.allocate(DEFAULT_LOG_PAGE_SIZE));
            freeBufferQ.offer(ByteBuffer.allocate(DEFAULT_LOG_PAGE_SIZE));
            partitionWriteBuffer.put(partitionId, ByteBuffer.allocate(DEFAULT_LOG_PAGE_SIZE));
            partitionReplicationQs.put(partitionId, new LinkedBlockingQueue<>(MAX_QUEUE_LENGTH));
            partitionMonitors.add(new Object());
        });
        freeBufferQ.offer(ByteBuffer.allocate(DEFAULT_LOG_PAGE_SIZE));
        freeBufferQ.offer(ByteBuffer.allocate(DEFAULT_LOG_PAGE_SIZE));
        freeBufferQ.offer(ByteBuffer.allocate(DEFAULT_LOG_PAGE_SIZE));
        freeBufferQ.offer(ByteBuffer.allocate(DEFAULT_LOG_PAGE_SIZE));
        freeBufferQ.offer(ByteBuffer.allocate(DEFAULT_LOG_PAGE_SIZE));
        freeBufferQ.offer(ByteBuffer.allocate(DEFAULT_LOG_PAGE_SIZE));

        this.datasetLifecycleManager = appRuntimeContextProvider.getDatasetLifecycleManager();
        startThreads();
        LOGGER.log(Level.INFO, "REPL: Streaming replication initialized!");
        try {
            refreshLocalResourceMap();
        } catch (HyracksDataException e) {
            e.printStackTrace();
        }
        //this.counters = new ArrayList<>(numPartitions);
    }

    @Override
    public void run() {
        Thread.currentThread().setName("Streaming Replication Thread");
    }

    public void handleLogReplication() throws IOException, ACIDException {
//        //set initial buffer size to a log buffer page size
//        inBuffer = ByteBuffer.allocate(replicationChannel.getLogManager().getLogPageSize());
//        while (true) {
//            //read a batch of logs
//            inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
//            //check if it is end of handshake (a single byte log)
//            if (inBuffer.remaining() == LOG_REPLICATION_END_HANKSHAKE_LOG_SIZE) {
//                break;
//            }
//
//            replayLogs(inBuffer);
//        }
    }

    private void replayLogs(ByteBuffer buffer) throws ACIDException {

    }

    @Override public void notifyLogReplicationRequester(LogRecord logRecord) {

    }

    @Override public SocketChannel getReplicationClientSocket() {
        return null;
    }

    public void flushAllWriteQs() {
        LOGGER.info("RECOVERY?? FLUSHING ALL WRITE BUFFERS AGAIN");
        partitionWriteBuffer.keySet().forEach(x -> {
            try {
                flushWriteBufferToQ(x);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    public void startThreads() {
        ExecutorService service = Executors.newFixedThreadPool(nodePartitions.size());
        nodePartitions.stream()
                .forEach(partition -> service.submit(new ReplicationMaterialzationThread(partition,
                        partitionReplicationQs.get(partition), partitionWriteBuffer.get(partition), partitionMonitors
                        .get(partition))));
        // TODO: Handle shutdown of the executor?
    }

    public void bufferCopy(ILogRecord logRecord) throws InterruptedException {
        int partitionId = logRecord.getResourcePartition();
        int logSize = logRecord.getRemoteLogSize(); // TODO: logSize or remoteLogSize?
        synchronized (partitionMonitors.get(partitionId)) {
            ByteBuffer writeBuffer = partitionWriteBuffer.get(partitionId);
            if (writeBuffer.remaining() < logSize) {
                LOGGER.log(Level.INFO, "REPL: RP" + partitionId + " write buffer is full, flushing it to Q");
                flushWriteBufferToQ(partitionId);
                writeBuffer = partitionWriteBuffer.get(partitionId);
            }
            logRecord.writeRemoteLogRecord(writeBuffer);
            partitionMonitors.get(partitionId).notify();
        }
    }

    private ByteBuffer getWriteBufferForPartition(int resourcePartition) {
        return partitionWriteBuffer.get(resourcePartition);
    }

    public synchronized void submit(ILogRecord logRecord) {
        try {
//            switch (logRecord.getLogType()) {
//                case LogType.UPDATE:
//                case LogType.ENTITY_COMMIT:
//                case LogType.UPSERT_ENTITY_COMMIT:
//                    bufferCopy(logRecord);
//                    break;
//                case LogType.JOB_COMMIT:
//                case LogType.ABORT:
//                    break;
//                case LogType.FLUSH:
//                    flushAllWriteQs();
//            }
            bufferCopy(logRecord);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private boolean flushWriteBufferToQ(int partition) throws InterruptedException {
        synchronized (partitionMonitors.get(partition)) {
            ByteBuffer currentBuffer = getWriteBufferForPartition(partition);
            if (currentBuffer.position() == 0) {
                return false;
            }
            Instant now = Instant.now();
            ByteBuffer freeBuffer = freeBufferQ.take();
            Instant end = Instant.now();
            LOGGER.log(Level.INFO, "REPL: Partition " + partition + " waited " + Duration.between(now, end).toMillis
                    () + "ms");
            freeBuffer.clear();
            partitionWriteBuffer.replace(partition, freeBuffer);
            currentBuffer.flip();
            partitionReplicationQs.get(partition).offer(currentBuffer);
            partitionMonitors.get(partition).notify();
        }
        return true;
    }

    public void refreshLocalResourceMap() throws HyracksDataException {
        this.resourceMap = ((PersistentLocalResourceRepository) localResourceRepository).loadAndGetAllResources();
    }

    private class ReplicationMaterialzationThread implements Runnable {

        private final int partition;
        private final BlockingQueue<ByteBuffer> jobQ;
        private ByteBuffer buffer;
        ILogRecord logRecord;
        private Object monitor;

        public ReplicationMaterialzationThread(int partition, BlockingQueue<ByteBuffer> jobQ, ByteBuffer buffer,
                Object monitor) {
            this.partition = partition;
            this.jobQ = jobQ;
            this.monitor = monitor;
            this.logRecord = new LogRecord();
        }

        @Override
        public void run() {
            String name = "RMT-" + partition;
            Thread.currentThread().setName(name);
            while (true) {
                try {
                    synchronized (monitor) {
                        while (jobQ.isEmpty()) {
                            LOGGER.log(Level.INFO, "REPL: " + name + " waiting because jobQ is empty");
                            monitor.wait();
                            LOGGER.log(Level.INFO, "REPL: " + name + ": Requesting a flush");
                            if (!flushWriteBufferToQ(partition)) {
                                LOGGER.log(Level.INFO, "REPL:" + name + ": Flush buffer failed for thread because of empty buffer");
                                continue;
                            } else {
                                LOGGER.log(Level.INFO, "REPL: " + name + ": Successful buffer switch");
                                break;
                            }
                        }
                    }
                    buffer = jobQ.take();
                    int counter = 0;
                    Instant start = Instant.now();
                    while (buffer.hasRemaining()) {
                        logRecord.readRemoteLog(buffer);
                        counter++;
                        LOGGER.log(Level.INFO, "REPL: " + name + " : read log record " + logRecord.getLogRecordForDisplay());
                        try {
                            materialize();
                        }
                        catch (HyracksDataException e) {
                                LOGGER.log(Level.SEVERE, "REPL: Replicationg a record failed!!!");
                                e.printStackTrace();
                        } catch (IndexException e) {
                                // TODO: change this to a catch all exception?
                                e.printStackTrace();
                        } catch (Exception e) {
                            LOGGER.log(Level.INFO, "REPL FAILED: " + name + " " +logRecord.getLogRecordForDisplay
                                    ());
                        }
                    }
                    Instant end = Instant.now();
                    LOGGER.log(Level.INFO, "REPL: STATS: " + name + " Num Logs in prev buffer: " + counter + " JOBQ "
                            + "has" + jobQ.size() + " buffers waiting! and time to complete " + Duration.between(start, end).toMillis());
                    buffer.clear();
                    freeBufferQ.offer(buffer);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        private ILSMIndex createIndex(ILogRecord logRecord) throws HyracksDataException {
            long resourceId = logRecord.getResourceId();
            LocalResource localResource = resourceMap.get(resourceId);
            if (localResource == null) {
                synchronized (resourceMapLock) {
                    localResource = resourceMap.get(resourceId);
                    if (localResource == null) {
                        LOGGER.log(Level.INFO, "Local resource " + resourceId + " not found!, refreshing the local "
                                + "resource repository.");
                        refreshLocalResourceMap();
                        localResource = resourceMap.get(resourceId);
                    }
                }
            }
            Resource localResourceMetadata = (Resource) localResource.getResource();
//            ILSMIndex index = ((LSMBTreeLocalResourceMetadata) localResourceMetadata)
//                    .createIndexInstance(txnSubSystem.getServiceContext(), localResource, true);
            ILSMIndex index = ((LSMBTreeLocalResourceMetadata) localResourceMetadata)
                    .createIndexInstance(txnSubSystem.getServiceContext(), localResource);
            datasetLifecycleManager.register(localResource.getPath(), index, true);
            datasetLifecycleManager.open(localResource.getPath());
            return index;
        }

        private void materialize() throws HyracksDataException, IndexException {
            long resourceId = logRecord.getResourceId();
            if (logRecord.getLogType() == LogType.UPDATE) {
                ILSMIndex index = (ILSMIndex) datasetLifecycleManager.getIndex(logRecord.getDatasetId(), logRecord
                        .getResourceId());
//                Resource localResourceMetadata = (Resource) localResource.getResource();
//                ILSMIndex index = (ILSMIndex) datasetLifecycleManager.get(localResource.getPath());
                if (index == null) {
                    index = createIndex(logRecord);

                }
                LOGGER.log(Level.INFO, "REPL: " + Thread.currentThread().getName() + " Redoing " + logRecord
                        .getLogRecordForDisplay());

                ILSMIndexAccessor indexAccessor = index
                        .createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);


                if (logRecord.getNewOp() == IndexOperation.INSERT.ordinal()) {
                    indexAccessor.insert(logRecord.getNewValue()); // TODO: Changed from forceInsert to insert.
                } else if (logRecord.getNewOp() == IndexOperation.DELETE.ordinal()) {
                    indexAccessor.delete(logRecord.getNewValue()); // TODO: Changed from forceDelete to delete.
                } else {
                    LOGGER.log(Level.SEVERE, "Unknown Optype to replicate!");
                }
            }
        }
    }
}