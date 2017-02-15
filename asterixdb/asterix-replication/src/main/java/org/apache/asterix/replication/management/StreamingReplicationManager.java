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
import org.apache.asterix.common.config.MetadataProperties;
import org.apache.asterix.common.transactions.*;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.asterix.transaction.management.service.recovery.RecoveryManager;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.common.file.ILocalResourceRepository;
import org.apache.hyracks.storage.common.file.LocalResource;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by msa on 1/17/17.
 */
public class StreamingReplicationManager {

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


    public StreamingReplicationManager(ITransactionSubsystem txnSubSystem, IAppRuntimeContextProvider
            asterixAppRuntimeContextProvider, MetadataProperties metadataProperties) {
        this.partitionReplicationQs = new ConcurrentHashMap<>();
        this.txnSubSystem = txnSubSystem;
        this.nodePartitions = metadataProperties.getClusterPartitions().keySet();
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

        this.datasetLifecycleManager = asterixAppRuntimeContextProvider.getDatasetLifecycleManager();
        startThreads();
        LOGGER.log(Level.INFO, "REPL: Streaming replication initialized!");
        try {
            refreshLocalResourceMap();
        } catch (HyracksDataException e) {
            e.printStackTrace();
        }
        //this.counters = new ArrayList<>(numPartitions);
    }

    public synchronized void refreshLocalResourceMap() throws HyracksDataException {
        this.resourceMap = ((PersistentLocalResourceRepository) localResourceRepository).loadAndGetAllResources();
    }

    public void startThreads() {
        ExecutorService service = Executors.newFixedThreadPool(nodePartitions.size());
        nodePartitions.stream()
                .forEach(partition -> service.submit(new ReplicationMaterialzationThread(partition,
                partitionReplicationQs.get(partition), partitionWriteBuffer.get(partition))));
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

    private synchronized ByteBuffer flipBuffers(ByteBuffer inactiveBuffer, int resourcePartition) {
        ByteBuffer activeBuffer;
        ByteBuffer temp;
        //synchronized ()

        throw new NotImplementedException();
    }

    private ByteBuffer getWriteBufferForPartition(int resourcePartition) {
        return partitionWriteBuffer.get(resourcePartition);
    }

    public synchronized void submit(ILogRecord logRecord) {
        try {
            bufferCopy(logRecord);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private ILogRecord readFromBuffer(ByteBuffer buffer) {
        ILogRecord logRecord = new LogRecord();
        synchronized (buffer) {
            // TODO: Construct LogRecord here from buffer.
        }
        return logRecord;
    }

    private boolean flushReplicationQ(int partition) {
        return false;
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

    private boolean requestForWork(int partition) {
        synchronized (partitionWriteBuffer.get(partition)) {
            while (getWriteBufferForPartition(partition).position() == 0) {
                try {
                    wait();
                    if (getWriteBufferForPartition(partition).position() != 0) {

                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        return false;
    }

    private class ReplicationMaterialzationThread implements Runnable {

        private final int partition;
        private final BlockingQueue<ByteBuffer> jobQ;
        private ByteBuffer buffer;
        ILogRecord logRecord;
        private Object completedMonitor;
        private boolean readyToMaterialize;

        public ReplicationMaterialzationThread(int partition, BlockingQueue<ByteBuffer> jobQ, ByteBuffer buffer) {
            this.partition = partition;
            this.jobQ = jobQ;
            this.logRecord = new LogRecord();
        }

        @Override
        public void run() {
            String name = "RMT-" + partition;
            Thread.currentThread().setName(name);
            while (true) {
                try {
                    synchronized (partitionMonitors.get(partition)) {
                        while (jobQ.isEmpty()) {
                            LOGGER.log(Level.INFO, "REPL: " + name + " waiting because jobQ is empty");
                            partitionMonitors.get(partition).wait();
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
                        } catch (Exception e) {
                            LOGGER.log(Level.INFO, "REPL: " + name + " " +logRecord.getLogRecordForDisplay());
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

        private void materialize() {
            long resourceId = logRecord.getResourceId();
            if (logRecord.getLogType() == LogType.UPDATE) {
                try {
                    LocalResource localResource = resourceMap.get(resourceId);
                    if (localResource == null) {
                        LOGGER.log(Level.INFO, "Local resource " + resourceId + " not found!, refreshing the local "
                                + "resource repository.");
                        synchronized (StreamingReplicationManager.class) {
                            refreshLocalResourceMap();
                        }
                    }
                    Resource localResourceMetadata = (Resource) localResource.getResource();
                    ILSMIndex index = (ILSMIndex) datasetLifecycleManager.get(localResource.getPath());
                    if (index == null) {
                        index = localResourceMetadata
                                .createIndexInstance(txnSubSystem.getAsterixAppRuntimeContextProvider(), localResource);
                        datasetLifecycleManager.register(localResource.getPath(), index);
                        datasetLifecycleManager.open(localResource.getPath());
                    }
                    LOGGER.log(Level.INFO, "REPL: " + Thread.currentThread().getName() + " Redoing " + logRecord
                            .getLogRecordForDisplay());
                    RecoveryManager.redo(datasetLifecycleManager, logRecord.getNewValue(), logRecord.getNewOp(),
                            logRecord.getDatasetId(), logRecord.getResourceId());
                } catch (HyracksDataException e) {
                    LOGGER.log(Level.SEVERE, "REPL: Replicationg a record failed!!!");
                    e.printStackTrace();
                }
            }
        }
    }
}