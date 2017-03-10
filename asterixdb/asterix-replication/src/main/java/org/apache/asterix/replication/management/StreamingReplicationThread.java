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

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.LogSource;
import org.apache.asterix.common.transactions.LogType;
import org.apache.asterix.common.transactions.Resource;
import org.apache.asterix.common.utils.TransactionUtil;
import org.apache.asterix.replication.functions.ReplicationProtocol;
import org.apache.asterix.replication.logging.RemoteLogMapping;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by msa on 2/9/17.
 */
@SuppressWarnings("Duplicates")
public class StreamingReplicationThread extends AbstractReplicationThread {

    private static final Logger LOGGER = Logger.getLogger(StreamingReplicationThread.class.getName());

    public StreamingReplicationThread(ReplicationChannel replicationChannel, SocketChannel socketChannel) {
        super(replicationChannel, socketChannel);
    }

    @Override
    public void run() {
        Thread.currentThread().setName("Replication Thread");
        try {
            ReplicationProtocol.ReplicationRequestType replicationFunction = ReplicationProtocol.getRequestType(socketChannel,
                    inBuffer);
            while (replicationFunction != ReplicationProtocol.ReplicationRequestType.GOODBYE) {
                LOGGER.info("Replication Function: " + replicationFunction.toString());
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

    @Override
    protected void handleLogReplication() throws IOException, ACIDException {
        //set initial buffer size to a log buffer page size
        LOGGER.info("REPL: handing log replication in StreamingReplicationThread");
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

    @Override
    public void processLogsBatch(ByteBuffer buffer) throws ACIDException {
        int upCount, ecCount, upsertCount, commitCount, abortCount, flushCount, unknown;
        upCount = ecCount = upsertCount = commitCount = abortCount = flushCount = unknown = 0;
        ILSMIndex index = null;
        Resource localResourceMetadata = null;
        int logSize = -1;


        while (buffer.hasRemaining()) {
            //get rid of log size
            logSize = inBuffer.getInt();
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
                    if (replicationChannel.nodeHostedPartitions.contains(remoteLog.getResourcePartition())) {
                        LOGGER.info("Before LSN: " + remoteLog.getLSN());
                        // TODO: What happens on failure here?
                        replicationChannel.logManager.log(remoteLog);
                        LOGGER.info("After LSN: " + remoteLog.getLSN());
                        replicationChannel.streamingReplicationManager.submit(remoteLog);
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
                    //updateLocalOpTracker();
                    jobTerminationLog.setReplicationThread(this);
                    jobTerminationLog.setLogSource(LogSource.REMOTE);
                    replicationChannel.logManager.log(jobTerminationLog);



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
                    replicationChannel.logManager.log(remoteLog);
                    //the log LSN value is updated by logManager.log(.) to a local value
                    flushLogMap.setLocalLSN(remoteLog.getLSN());
                    flushLogMap.numOfFlushedIndexes.set(remoteLog.getNumOfFlushedIndexes());
                    replicationChannel.replicaUniqueLSN2RemoteMapping.put(flushLogMap.getNodeUniqueLSN(), flushLogMap);
                    synchronized (replicationChannel.flushLogslock) {
                        replicationChannel.flushLogslock.notify();
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
}
