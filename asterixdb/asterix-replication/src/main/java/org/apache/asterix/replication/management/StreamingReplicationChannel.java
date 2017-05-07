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

import org.apache.asterix.common.config.ReplicationProperties;
import org.apache.asterix.common.context.TransactionSubsystemProvider;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.replication.IReplicaResourcesManager;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.replication.IReplicationThread;
import org.apache.asterix.common.transactions.*;
import org.apache.asterix.common.utils.TransactionUtil;
import org.apache.asterix.replication.functions.ReplicationProtocol;
import org.apache.asterix.replication.logging.RemoteLogMapping;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.util.StorageUtil;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * This thread is responsible for managing replication requests.
 */
public class StreamingReplicationChannel extends AbstractReplicationChannel {

    private static final Logger LOGGER = Logger.getLogger(StreamingReplicationChannel.class.getName());
    private static final boolean DEBUG_MODE = true;

    private SocketChannel socketChannel;
    private StreamingReplicationThread streamingReplicationThread;
    private ByteBuffer inBuffer;
    private ByteBuffer outBuffer;
    private static final int LOG_REPLICATION_END_HANKSHAKE_LOG_SIZE = 1;

    public StreamingReplicationChannel(String nodeId, ReplicationProperties replicationProperties, ILogManager logManager,
            IReplicaResourcesManager replicaResoucesManager, IReplicationManager replicationManager,
            INCServiceContext ncServiceContext, IAppRuntimeContextProvider asterixAppRuntimeContextProvider) {
        super(nodeId, replicationProperties, logManager, replicaResoucesManager, replicationManager,
                ncServiceContext, asterixAppRuntimeContextProvider);
        this.streamingReplicationThread = new StreamingReplicationThread(appContextProvider, replicaResoucesManager);
        this.inBuffer = ByteBuffer.allocate(StorageUtil.getIntSizeInBytes(4, StorageUtil.StorageUnit.KILOBYTE));
    }

    @Override
    public void run() {
        Thread.currentThread().setName("Streaming Replication Channel Thread");

        String nodeIP = replicationProperties.getReplicaIPAddress(localNodeID);
        int dataPort = replicationProperties.getDataReplicationPort(localNodeID);
        try {
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(true);
            InetSocketAddress replicationChannelAddress = new InetSocketAddress(InetAddress.getByName(nodeIP),
                    dataPort);
            serverSocketChannel.socket().bind(replicationChannelAddress);

            LOGGER.log(Level.INFO, "opened Replication Channel @ IP Address: " + nodeIP + ":" + dataPort);

            //start accepting replication requests
            while (true) {
                socketChannel = serverSocketChannel.accept();
                socketChannel.configureBlocking(true);
                //start a new thread to handle the request
                //replicationThreads.execute(new StreamingReplicationThread(socketChannel));
                //replicationThreads.execute(new StreamingReplicationThread());
                handleIncomingRequest(socketChannel);
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Could not open replication channel @ IP Address: " + nodeIP + ":" + dataPort, e);
        }
    }

    private void handleIncomingRequest(SocketChannel socketChannel) {
        this.inBuffer = ByteBuffer.allocate(StorageUtil.getIntSizeInBytes(4, StorageUtil.StorageUnit.KILOBYTE));
        try {
            ReplicationProtocol.ReplicationRequestType replicationFunction = ReplicationProtocol.getRequestType(socketChannel,
                    inBuffer);
            while (replicationFunction != ReplicationProtocol.ReplicationRequestType.GOODBYE) {
                switch (replicationFunction) {
                    case REPLICATE_LOG:
                        handleLogReplication(socketChannel);
                        break;
                    default:
                        LOGGER.info("Unsupported replication request type: " + replicationFunction.name());
                }
                replicationFunction = ReplicationProtocol.getRequestType(socketChannel, inBuffer);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            LOGGER.info("Unexpected error during replication");
        }
        finally {
            if (socketChannel.isOpen()) {
                try {
                    socketChannel.close();
                } catch (IOException e) {
                    LOGGER.log(Level.INFO, "Failed to close replication socket.", e);
                }
            }
        }
    }

    private void handleLogReplication(SocketChannel socketChannel) throws IOException, ACIDException {
        LogRecord remoteLog = new LogRecord();
        inBuffer = ByteBuffer.allocate(getLogManager().getLogPageSize());
        while (true) {
            //read a batch of logs
            inBuffer = ReplicationProtocol.readRequest(socketChannel, inBuffer);
            //check if it is end of handshake (a single byte log)
            if (inBuffer.remaining() == LOG_REPLICATION_END_HANKSHAKE_LOG_SIZE) {
                break;
            }

            while (inBuffer.hasRemaining()) {
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
                        if (nodeHostedPartitions.contains(remoteLog.getResourcePartition())) {
                            getLogManager().log(remoteLog);
                            if (DEBUG_MODE) {
                                LOGGER.info("Inserting into replica: " + remoteLog.getPKHashValue());
                            }
                            try {
                                streamingReplicationThread.submit(remoteLog);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        break;
                    case LogType.JOB_COMMIT:
                    case LogType.ABORT:
                        LogRecord jobTerminationLog = new LogRecord();
                        TransactionUtil.formJobTerminateLogRecord(jobTerminationLog, remoteLog.getJobId(),
                                remoteLog.getLogType() == LogType.JOB_COMMIT);
                        //jobTerminationLog.setReplicationThread(streamingReplicationThread);
                        jobTerminationLog.setLogSource(LogSource.REMOTE);
                        getLogManager().log(jobTerminationLog);
                        break;
                    case LogType.FLUSH:
                        //store mapping information for flush logs to use them in incoming LSM components.
                        RemoteLogMapping flushLogMap = new RemoteLogMapping();
                        flushLogMap.setRemoteNodeID(remoteLog.getNodeId());
                        flushLogMap.setRemoteLSN(remoteLog.getLSN());
                        getLogManager().log(remoteLog);
                        //the log LSN value is updated by logManager.log(.) to a local value
                        flushLogMap.setLocalLSN(remoteLog.getLSN());
                        flushLogMap.numOfFlushedIndexes.set(remoteLog.getNumOfFlushedIndexes());
//                        replicaUniqueLSN2RemoteMapping.put(flushLogMap.getNodeUniqueLSN(), flushLogMap);
//                        synchronized (replicationChannel.flushLogslock) {
//                            flushLogslock.notify();
//                        }
                        break;
                    default:
                        LOGGER.severe("Unsupported LogType: " + remoteLog.getLogType());
                }
            }
        }
    }

    @Override
    public void close() throws IOException {

    }

    private class ReplicationMaterializationThread extends Thread {

        @Override
        public void run() {

        }

        public boolean submit(ILogRecord logRecord) {
            return true;
        }

    }
}
