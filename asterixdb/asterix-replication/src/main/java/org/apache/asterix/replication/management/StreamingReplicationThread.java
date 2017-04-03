package org.apache.asterix.replication.management;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.replication.IReplicationChannel;
import org.apache.asterix.common.replication.IReplicationThread;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.replication.functions.ReplicationProtocol;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;

/**
 * Created by msa on 3/24/17.
 */
public class StreamingReplicationThread implements IReplicationThread {

    private final SocketChannel socketChannel;
    private ByteBuffer inBuffer;



    public StreamingReplicationThread(IReplicationChannel replicationChannel, SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    @Override
    public void run() {
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
}
