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
import org.apache.asterix.common.replication.IReplicaResourcesManager;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.transactions.*;
import org.apache.asterix.replication.logging.RemoteLogMapping;
import org.apache.asterix.replication.storage.LSMComponentLSNSyncTask;
import org.apache.asterix.replication.storage.LSMComponentProperties;
import org.apache.hyracks.api.application.INCServiceContext;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * This thread is responsible for managing replication requests.
 */
public class StreamingReplicationChannel extends AbstractReplicationChannel {

    private static final Logger LOGGER = Logger.getLogger(StreamingReplicationChannel.class.getName());

    protected final Map<String, RemoteLogMapping> replicaUniqueLSN2RemoteMapping;
    protected final Map<String, LSMComponentProperties> lsmComponentId2PropertiesMap;
    protected final ReplicationNotifier replicationNotifier;
    protected final LogReplayManager logReplayManager;

    public StreamingReplicationChannel(String nodeId, ReplicationProperties replicationProperties, ILogManager logManager,
            IReplicaResourcesManager replicaResoucesManager, IReplicationManager replicationManager,
            INCServiceContext ncServiceContext, IAppRuntimeContextProvider asterixAppRuntimeContextProvider) {
        super(nodeId, replicationProperties, logManager, replicaResoucesManager, replicationManager,
                ncServiceContext, asterixAppRuntimeContextProvider);
        lsmComponentId2PropertiesMap = new ConcurrentHashMap<>();
        replicaUniqueLSN2RemoteMapping = new ConcurrentHashMap<>();
        replicationNotifier = new ReplicationNotifier();
        this.logReplayManager = new LogReplayManager(appContextProvider, replicaResoucesManager);
    }

    public LogReplayManager getLogReplayManager() {
        return logReplayManager;
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
            replicationNotifier.start();
            LOGGER.log(Level.INFO, "opened Replication Channel @ IP Address: " + nodeIP + ":" + dataPort);

//            lsmComponentLSNMappingService.start();
            //start accepting replication requests
            while (true) {
                socketChannel = serverSocketChannel.accept();
                socketChannel.configureBlocking(true);
                //start a new thread to handle the request
                replicationThreads.execute(new ActiveReplicationThread(this, socketChannel));
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Could not open replication channel @ IP Address: " + nodeIP + ":" + dataPort, e);
        }
    }

    @Override
    public void close() throws IOException {
        if (!serverSocketChannel.isOpen()) {
            serverSocketChannel.close();
            LOGGER.log(Level.INFO, "Replication channel closed.");
        }
    }
}
