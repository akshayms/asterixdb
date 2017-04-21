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
package org.apache.asterix.common.replication;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.config.ClusterProperties;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ChainedDeclusteringReplicationStrategy implements IReplicationStrategy {

    private static final Logger LOGGER = Logger.getLogger(ChainedDeclusteringReplicationStrategy.class.getName());
    private int replicationFactor;
    private boolean isStreamingReplication = true;

    @Override
    public boolean isMatch(int datasetId) {
        if (datasetId < MetadataIndexImmutableProperties.FIRST_AVAILABLE_USER_DATASET_ID && datasetId >= 0) {
            return true;
        }
        return true && !isStreamingReplication;
    }

    @Override
    public boolean isMatchForFailover(int datasetId) {
        return true;
    }

    @Override
    public boolean replicateLog() {
        return true;
    }

    @Override
    public boolean replicateLog(int datasetId) {
        return  isStreamingReplication || (isMatch(datasetId));
    }

    @Override
    public Set<Replica> getRemoteReplicas(String nodeId) {
        Set<Replica> remoteReplicas = new HashSet<>();
        Cluster cluster = ClusterProperties.INSTANCE.getCluster();
        int numberOfRemoteReplicas = replicationFactor - 1;
        int nodeIndex = ClusterProperties.INSTANCE.getNodeIndex(nodeId);

        if (nodeIndex == -1) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Could not find node " + nodeId + " in cluster configurations");
            }
            return Collections.emptySet();
        }

        //find nodes to the right of this node
        while (remoteReplicas.size() != numberOfRemoteReplicas) {
            remoteReplicas.add(new Replica(cluster.getNode().get(++nodeIndex % cluster.getNode().size())));
        }

        return remoteReplicas;
    }

    @Override
    public Set<Replica> getRemotePrimaryReplicas(String nodeId) {
        Set<Replica> clientReplicas = new HashSet<>();
        Cluster cluster = ClusterProperties.INSTANCE.getCluster();
        final int remotePrimaryReplicasCount = replicationFactor - 1;

        int nodeIndex = ClusterProperties.INSTANCE.getNodeIndex(nodeId);

        //find nodes to the left of this node
        while (clientReplicas.size() != remotePrimaryReplicasCount) {
            clientReplicas.add(new Replica(cluster.getNode().get(Math.abs(--nodeIndex % cluster.getNode().size()))));
        }

        return clientReplicas;
    }

    @Override
    public ChainedDeclusteringReplicationStrategy from(Cluster cluster) throws HyracksDataException {
        if (cluster.getHighAvailability().getDataReplication().getReplicationFactor() == null) {
            throw new RuntimeDataException(ErrorCode.INVALID_CONFIGURATION, "Replication factor must be specified.");
        }
        ChainedDeclusteringReplicationStrategy cd = new ChainedDeclusteringReplicationStrategy();
        cd.replicationFactor = cluster.getHighAvailability().getDataReplication().getReplicationFactor().intValue();
        return cd;
    }
}