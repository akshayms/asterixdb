package org.apache.asterix.replication.management;

import org.apache.asterix.common.replication.Replica;
import org.apache.asterix.common.replication.ReplicaEvent;
import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.hyracks.api.replication.IReplicationJob;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.logging.Logger;

/**
 * This class manages active/streaming replication
 */
public class StreamingReplicationManager extends AbstractReplicationManager {

    private static final Logger LOGGER = Logger.getLogger(StreamingReplicationManager.class.getName());
    private static final boolean DEBUG_MODE = true;

    @Override public void start() {

    }

    @Override public void dumpState(OutputStream os) throws IOException {

    }

    @Override public void stop(boolean dumpState, OutputStream ouputStream) throws IOException {

    }

    @Override public void submitJob(IReplicationJob job) throws IOException {
        if (DEBUG_MODE) {
            LOGGER.info("Submitting job: " + job);
        }
        if (job.getExecutionType() == IReplicationJob.ReplicationExecutionType.ASYNC) {

        }

    }

    @Override public boolean isReplicationEnabled() {
        return false;
    }

    @Override public void replicateLog(ILogRecord logRecord) throws InterruptedException {

    }

    @Override public boolean hasBeenReplicated(ILogRecord logRecord) {
        return false;
    }

    @Override public void requestReplicaFiles(String remoteReplicaId, Set<Integer> partitionsToRecover,
            Set<String> existingFiles) throws IOException {

    }

    @Override public long getMaxRemoteLSN(Set<String> remoteReplicaIds) throws IOException {
        return 0;
    }

    @Override public int getActiveReplicasCount() {
        return 0;
    }

    @Override public Set<String> getDeadReplicasIds() {
        return null;
    }

    @Override public void startReplicationThreads() throws InterruptedException {

    }

    @Override public void initializeReplicasState() {

    }

    @Override public void updateReplicaInfo(Replica replica) {

    }

    @Override public Set<String> getActiveReplicasIds() {
        return null;
    }

    @Override public void reportReplicaEvent(ReplicaEvent event) {

    }

    @Override public void requestFlushLaggingReplicaIndexes(long nonSharpCheckpointTargetLSN) throws IOException {

    }

    @Override public void replicateTxnLogBatch(ByteBuffer buffer) {

    }
}
