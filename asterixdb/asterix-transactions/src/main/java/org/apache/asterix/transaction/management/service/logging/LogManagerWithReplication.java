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
package org.apache.asterix.transaction.management.service.logging;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.replication.IReplicationStrategy;
import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.LogSource;
import org.apache.asterix.common.transactions.LogType;
import org.apache.asterix.transaction.management.service.transaction.TransactionSubsystem;

import java.util.logging.Logger;

public class LogManagerWithReplication extends LogManager {

    private IReplicationManager replicationManager;
    private final IReplicationStrategy replicationStrategy;
    private final Set<Integer> replicatedJob = ConcurrentHashMap.newKeySet();

    private static final Logger LOGGER = Logger.getLogger(LogManagerWithReplication.class.getName());

    public LogManagerWithReplication(TransactionSubsystem txnSubsystem, IReplicationStrategy replicationStrategy) {
        super(txnSubsystem);
        this.replicationStrategy = replicationStrategy;
    }

    @Override
    public void log(ILogRecord logRecord) throws ACIDException {
        boolean shouldReplicate = logRecord.getLogSource() == LogSource.LOCAL && logRecord.getLogType() != LogType.WAIT;
        if (shouldReplicate) {
            switch (logRecord.getLogType()) {
                case LogType.ENTITY_COMMIT:
                case LogType.UPSERT_ENTITY_COMMIT:
                case LogType.UPDATE:
                case LogType.FLUSH:
                    shouldReplicate = replicationStrategy.isMatch(logRecord.getDatasetId());
                    if (shouldReplicate && !replicatedJob.contains(logRecord.getJobId())) {
                        replicatedJob.add(logRecord.getJobId());
                    }
                    break;
                case LogType.JOB_COMMIT:
                case LogType.ABORT:
                    shouldReplicate = replicatedJob.remove(logRecord.getJobId());
                    break;
                default:
                    shouldReplicate = false;
            }
        }
        logRecord.setReplicated(shouldReplicate);

         //Remote flush logs do not need to be flushed separately since they may not trigger local flush
        if (logRecord.getLogType() == LogType.FLUSH && logRecord.getLogSource() == LogSource.LOCAL) {
            flushLogsQ.offer(logRecord);
            return;
        }

        appendToLogTail(logRecord);
    }

    @Override
    protected void appendToLogTail(ILogRecord logRecord) throws ACIDException {
        syncAppendToLogTail(logRecord);

        if (logRecord.isReplicated()) {
            try {
                replicationManager.replicateLog(logRecord);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (logRecord.getLogSource() == LogSource.LOCAL) {
            if ((logRecord.getLogType() == LogType.JOB_COMMIT || logRecord.getLogType() == LogType.ABORT
                    || logRecord.getLogType() == LogType.WAIT) && !logRecord.isFlushed()) {
                synchronized (logRecord) {
                    while (!logRecord.isFlushed()) {
                        try {
                            logRecord.wait();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }

                    //wait for job Commit/Abort ACK from replicas
                    if (logRecord.isReplicated() && (logRecord.getLogType() == LogType.JOB_COMMIT
                            || logRecord.getLogType() == LogType.ABORT)) {
                        while (!replicationManager.hasBeenReplicated(logRecord)) {
                            try {
                                logRecord.wait();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    protected synchronized void syncAppendToLogTail(ILogRecord logRecord) throws ACIDException {
        if (logRecord.getLogSource() == LogSource.LOCAL && logRecord.getLogType() != LogType.FLUSH) {
            ITransactionContext txnCtx = logRecord.getTxnCtx();
            if (txnCtx.getTxnState() == ITransactionManager.ABORTED && logRecord.getLogType() != LogType.ABORT) {
                throw new ACIDException(
                        "Aborted job(" + txnCtx.getJobId() + ") tried to write non-abort type log record.");
            }
        }

        final int logRecordSize = logRecord.getLogSize();
        // Make sure the log will not exceed the log file size
        if (getLogFileOffset(appendLSN.get()) + logRecordSize >= logFileSize) {
            prepareNextLogFile();
            prepareNextPage(logRecordSize);
        } else if (!appendPage.hasSpace(logRecordSize)) {
            prepareNextPage(logRecordSize);
        }
        appendPage.appendWithReplication(logRecord, appendLSN.get());

        if (logRecord.getLogType() == LogType.FLUSH) {//|| logRecord.getLogType() == LogType.UPDATE || logRecord
                //.getLogType() == LogType.ENTITY_COMMIT) {
            logRecord.setLSN(appendLSN.get());
        }

        if (logRecord.getLogSource() == LogSource.REMOTE && (logRecord.getLogType() == LogType.UPDATE || logRecord
                .getLogType() == LogType.ENTITY_COMMIT)) {
            long lsn = appendLSN.get();
            LOGGER.info("REPL: setting append LSN to " + lsn);
            logRecord.setLSN(lsn);
        }
        appendLSN.addAndGet(logRecordSize);
    }

    @Override
    public void setReplicationManager(IReplicationManager replicationManager) {
        this.replicationManager = replicationManager;
    }

    public void replicate() {

    }
}
