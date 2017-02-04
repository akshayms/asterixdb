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

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.transactions.*;
import org.apache.asterix.runtime.util.AppContextInfo;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.asterix.transaction.management.service.recovery.RecoveryManager;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.common.file.ILocalResourceRepository;
import org.apache.hyracks.storage.common.file.LocalResource;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
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
    private static final int DEFAULT_LOG_PAGE_SIZE = 2000;
    private static final int MAX_QUEUE_LENGTH = 15;
    private Object monitor = new Object();
    private final int ACTIVE_LOG_INDEX = 0;
    private final int INACTIVE_LOG_INDEX = 1;

    private final ILocalResourceRepository localResourceRepository;
    private final IDatasetLifecycleManager datasetLifecycleManager;


    public StreamingReplicationManager(ITransactionSubsystem txnSubSystem, IAppRuntimeContextProvider
            asterixAppRuntimeContextProvider) {
        this.partitionReplicationQs = new HashMap<>();
        this.txnSubSystem = txnSubSystem;
        this.nodePartitions = AppContextInfo.INSTANCE.getMetadataProperties().getClusterPartitions().keySet();
        this.numPartitions = nodePartitions.size();
        this.partitionWriteBuffer = new HashMap<>();
        this.partitionMonitors = new ArrayList<>();
        this.freeBufferQ = new LinkedBlockingQueue<>();
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
        this.localResourceRepository = (PersistentLocalResourceRepository) txnSubSystem
                .getAsterixAppRuntimeContextProvider().getLocalResourceRepository();
        this.datasetLifecycleManager = asterixAppRuntimeContextProvider.getDatasetLifecycleManager();
        startThreads();
        LOGGER.log(Level.INFO, "REPL: Streaming replication initialized!");
    }

    public void startThreads() {
        ExecutorService service = Executors.newFixedThreadPool(nodePartitions.size());
        nodePartitions.stream()
                .forEach(partition -> service.submit(new ReplicationMaterialzationThread(partition,
                partitionReplicationQs.get(partition), partitionWriteBuffer.get(partition))));
    }

    public void reconstructAndReplayTest(ByteBuffer buffer) {
        // TODO: Test if this method will work in replaying the logs after cloning the new value into buffer.
        ILogRecord record = new LogRecord();
        int size = buffer.getInt();
        record.readRemoteLog(buffer);
        // replay the log record here. (call redo or materialize?)
    }

    public void bufferCopy(ILogRecord logRecord) {
        int resourcePartition = logRecord.getResourcePartition();
        ByteBuffer inactiveBuffer = getWriteBufferForPartition(logRecord.getResourcePartition());
        BlockingQueue replQ = partitionReplicationQs.get(resourcePartition);
        int size = logRecord.getLogSize();
        LOGGER.log(Level.INFO, "REPL: Writing log to buffer: " + logRecord.getLogRecordForDisplay());
        if (inactiveBuffer.remaining() < size + Integer.BYTES || inactiveBuffer.position() > 100) {
            LOGGER.log(Level.INFO, "REPL: LOG BUFFER IS FULL!");
            //inactiveBuffer.putInt(size);
            synchronized (partitionWriteBuffer) {
                inactiveBuffer.flip();
                replQ.offer(inactiveBuffer);
                partitionWriteBuffer.remove(resourcePartition);
                try {
                    ByteBuffer newBuffer = freeBufferQ.take();
                    partitionWriteBuffer.put(resourcePartition, newBuffer);
                    // overwriting old buffer placed in q?
                    inactiveBuffer = newBuffer;
                    inactiveBuffer.clear();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        logRecord.writeRemoteLogRecord(inactiveBuffer);
//        replQ.offer(inactiveBuffer);
//        try {
//            partitionWriteBuffer.put(resourcePartition, freeBufferQ.take());
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        // flush the buffer periodically if buffer does not get full?
    }

//    private void copyTupleToBuffer(ITupleReference newValue, ByteBuffer inactiveBuffer) {
//        SimpleTupleWriter.INSTANCE.writeTuple(newValue, inactiveBuffer, inactiveBuffer.position());
//    }

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
        bufferCopy(logRecord);
    }

    private int getLocalPartitions() {
        return AppContextInfo.INSTANCE.getMetadataProperties().getClusterPartitions().keySet().size();
    }

    private ILogRecord readFromBuffer(ByteBuffer buffer) {
        ILogRecord logRecord = new LogRecord();
        synchronized (buffer) {
            // TODO: Construct LogRecord here from buffer.
        }
        return logRecord;
    }



    private class ReplicationMaterialzationThread implements Runnable {

        private final int partition;
        private final BlockingQueue<ByteBuffer> jobQ;
        private ByteBuffer buffer;
        ILogRecord logRecord;
        private Object completedMonitor;
        private boolean readyToMaterialize;

        public ReplicationMaterialzationThread(int partition, BlockingQueue jobQ, ByteBuffer buffer) {
            this.partition = partition;
            this.jobQ = jobQ;
            this.logRecord = new LogRecord();
        }

        @Override
        public void run() {
            while (true) {
                try {
                    buffer = jobQ.take();
                    LOGGER.log(Level.INFO, "REPL: read from q!!!");
                    while (buffer.position() <= buffer.limit()) {
                        logRecord.readRemoteLog(buffer);
                        LOGGER.log(Level.INFO, "REPL: Re-read" + logRecord.getLogRecordForDisplay());
                        materialize();
                    }
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
                    LocalResource localResource = ((PersistentLocalResourceRepository) localResourceRepository).loadAndGetAllResources().get(resourceId);
                    Resource localResourceMetadata = (Resource) localResource.getResource();
                    ILSMIndex index = (ILSMIndex) datasetLifecycleManager.get(localResource.getPath());
                    if (index == null) {
                        index = localResourceMetadata
                                .createIndexInstance(txnSubSystem.getAsterixAppRuntimeContextProvider(), localResource);
                        datasetLifecycleManager.register(localResource.getPath(), index);
                        datasetLifecycleManager.open(localResource.getPath());
                    }
                    LOGGER.log(Level.INFO, "REPL: Redoing " + logRecord.getLogRecordForDisplay());
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