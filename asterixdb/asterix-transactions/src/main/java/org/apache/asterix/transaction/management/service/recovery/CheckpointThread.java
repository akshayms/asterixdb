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
package org.apache.asterix.transaction.management.service.recovery;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.transactions.ICheckpointManager;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * A daemon thread that periodically attempts to perform checkpoints.
 * A checkpoint attempt is made when the volume of transaction logs written
 * since the last successful checkpoint exceeds a certain threshold.
 */
public class CheckpointThread extends Thread {

    private static final Logger LOGGER = Logger.getLogger(CheckpointThread.class.getName());
    private long lsnThreshold;
    private long checkpointTermInSecs;

    private final ILogManager logManager;
    private final ICheckpointManager checkpointManager;
    private volatile boolean shouldRun = true;

    public CheckpointThread(ICheckpointManager checkpointManager, ILogManager logManager, long lsnThreshold,
            long checkpointTermInSecs) {
        this.checkpointManager = checkpointManager;
        this.logManager = logManager;
        this.lsnThreshold = lsnThreshold;
        this.checkpointTermInSecs = checkpointTermInSecs;
        setDaemon(true);
    }

    @Override
    public void run() {
        Thread.currentThread().setName("Checkpoint Thread");
        long currentCheckpointAttemptMinLSN;
        long lastCheckpointLSN = -1;
        long currentLogLSN;
        long targetCheckpointLSN;
        while (shouldRun) {
            try {
                sleep(checkpointTermInSecs * 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (!shouldRun) {
                return;
            }
            if (lastCheckpointLSN == -1) {
                try {
                    //Since the system just started up after sharp checkpoint,
                    //last checkpoint LSN is considered as the min LSN of the current log partition
                    lastCheckpointLSN = logManager.getReadableSmallestLSN();
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "Error getting smallest readable LSN", e);
                    lastCheckpointLSN = 0;
                }
            }

            //1. get current log LSN
            currentLogLSN = logManager.getAppendLSN();

            //2. if current log LSN - previous checkpoint > threshold, do checkpoint
            if (currentLogLSN - lastCheckpointLSN > lsnThreshold) {
                try {
                    // in check point:
                    //1. get minimum first LSN (MFL) from open indexes.
                    //2. if current MinFirstLSN < targetCheckpointLSN, schedule async flush for any open index witch has first LSN < force flush delta
                    //3. next time checkpoint comes, it will be able to remove log files which have end range less than current targetCheckpointLSN

                    targetCheckpointLSN = lastCheckpointLSN + lsnThreshold;
                    currentCheckpointAttemptMinLSN = checkpointManager.tryCheckpoint(targetCheckpointLSN);

                    //checkpoint was completed at target LSN or above
                    if (currentCheckpointAttemptMinLSN >= targetCheckpointLSN) {
                        lastCheckpointLSN = currentCheckpointAttemptMinLSN;
                    }
                } catch (HyracksDataException e) {
                    LOGGER.log(Level.SEVERE, "Error during checkpoint", e);
                }
            }
        }
    }

    public void shutdown() {
        shouldRun = false;
    }
}
