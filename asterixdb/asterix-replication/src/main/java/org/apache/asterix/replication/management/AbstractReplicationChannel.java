package org.apache.asterix.replication.management;

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.config.ReplicationProperties;
import org.apache.asterix.common.replication.IReplicaResourcesManager;
import org.apache.asterix.common.replication.IReplicationChannel;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.transactions.IAppRuntimeContextProvider;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.replication.functions.ReplicationProtocol;
import org.apache.asterix.replication.storage.ReplicaResourcesManager;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.hyracks.api.application.INCServiceContext;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class AbstractReplicationChannel extends Thread implements IReplicationChannel {

    private static final Logger LOGGER = Logger.getLogger(AbstractReplicationChannel.class.getName());

    protected final ReplicationProperties replicationProperties;
    protected final IReplicationManager replicationManager;
    protected final IAppRuntimeContextProvider appContextProvider;
    protected final IDatasetLifecycleManager dsLifecycleManager;
    protected final PersistentLocalResourceRepository localResourceRep;
    protected SocketChannel socketChannel = null;
    protected ServerSocketChannel serverSocketChannel = null;
    protected final String localNodeID;
    protected final LinkedBlockingQueue<LogRecord> pendingNotificationRemoteLogsQ;
    protected final ExecutorService replicationThreads;
    protected final ReplicaResourcesManager replicaResourcesManager;

    private final ILogManager logManager;

    public AbstractReplicationChannel(String nodeId, ReplicationProperties replicationProperties, ILogManager logManager,
            IReplicaResourcesManager replicaResoucesManager, IReplicationManager replicationManager,
            INCServiceContext ncServiceContext, IAppRuntimeContextProvider asterixAppRuntimeContextProvider) {
        this.localNodeID = nodeId;
        this.logManager = logManager;
        this.replicationProperties = replicationProperties;
        this.replicaResourcesManager = (ReplicaResourcesManager) replicaResoucesManager;
        this.replicationManager = replicationManager;
        this.appContextProvider = asterixAppRuntimeContextProvider;
        this.dsLifecycleManager = asterixAppRuntimeContextProvider.getDatasetLifecycleManager();
        this.localResourceRep = (PersistentLocalResourceRepository) asterixAppRuntimeContextProvider
                .getLocalResourceRepository();
        this.pendingNotificationRemoteLogsQ = new LinkedBlockingQueue<>();
        this.replicationThreads = Executors.newCachedThreadPool(ncServiceContext.getThreadFactory());

    }

    /**
     * This thread is responsible for sending JOB_COMMIT/ABORT ACKs to replication clients.
     */
    protected class ReplicationNotifier extends Thread {
        @Override
        public void run() {
            Thread.currentThread().setName("ReplicationNotifier Thread");
            while (true) {
                try {
                    LogRecord logRecord = pendingNotificationRemoteLogsQ.take();
                    //send ACK to requester
                    logRecord.getReplicationThread().getReplicationClientSocket().socket().getOutputStream()
                            .write((localNodeID + ReplicationProtocol.JOB_REPLICATION_ACK + logRecord.getJobId()
                                    + System.lineSeparator()).getBytes());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.log(Level.WARNING, "Failed to send job replication ACK", e);
                    }
                }
            }
        }
    }

    @Override
    public ILogManager getLogManager() {
        return logManager;
    }

    @Override
    public IAppRuntimeContextProvider getAppRuntimeContextProvider() { return appContextProvider; }

    @Override
    public IReplicaResourcesManager getReplicaResourcesManager() { return replicaResourcesManager; }


}
