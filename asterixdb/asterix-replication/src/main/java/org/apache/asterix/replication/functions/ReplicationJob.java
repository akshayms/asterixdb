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

package org.apache.asterix.replication.functions;

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.context.DatasetLifecycleManager;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.transaction.management.service.recovery.RecoveryManager;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.tuples.SimpleTupleReference;
import org.apache.hyracks.storage.am.common.tuples.SimpleTupleWriter;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

public class ReplicationJob implements Runnable {

    private final long resourceId;
    private final int jobId;
    private final int datasetId;
    private final int PKHashValue;
    private final byte logType;
    private final ITupleReference newValue;
    private final byte newOp;
    private final IDatasetLifecycleManager datasetLifecycleManager;

    private static int sleepDuration = 2000000000;
    private static final int sleepDampFactor = 2;

    private static final Logger LOGGER = Logger.getLogger(ReplicationJob.class.getName());

    public ReplicationJob(long resourceId, int jobId, int datasetId, int PKHashValue, byte logType,
            ITupleReference newValue, byte newOp, IDatasetLifecycleManager datasetLifecycleManager) {
        this.resourceId = resourceId;
        this.jobId = jobId;
        this.datasetId = datasetId;
        this.PKHashValue = PKHashValue;
        this.logType = logType;
        this.newOp = newOp;
        this.datasetLifecycleManager = datasetLifecycleManager;
        ByteBuffer buffer = ByteBuffer.allocate(SimpleTupleWriter.INSTANCE.getCopySpaceRequired(newValue) * 4);
        int before = buffer.position();
        // Clone new tuple contents.
        this.newValue = new SimpleTupleReference();

        ((SimpleTupleReference) this.newValue).resetByTupleOffset(buffer.array(), before);
        ((SimpleTupleReference) this.newValue).setFieldCount(newValue.getFieldCount());
        SimpleTupleWriter.INSTANCE.writeTuple(newValue, buffer, before);
        //((SimpleTupleReference) this.newValue).resetByTupleOffset(buffer.array(), before);
        // TODO: Alt:
        // method 1 -> Use system.arraycopy
        // method 2 ->
    }

    @Override
    public void run() {
        try {
            sleepDuration /= sleepDampFactor;
            LOGGER.info("Sleeping for "  + sleepDuration + " PK : " + PKHashValue);
            Thread.sleep(sleepDuration);
            LOGGER.info("Replicating PKHash: " + PKHashValue);
            RecoveryManager.redo(datasetLifecycleManager, newValue, newOp, datasetId, resourceId);
            LOGGER.info("Replicated: " + PKHashValue);
            ((DatasetLifecycleManager) datasetLifecycleManager).dumpState(System.out);
            //datasetLifecycleManager.close(resourceAbsolutePath);
        } catch (Exception e) {
            LOGGER.info("Failed to replicate: " + PKHashValue);
            e.printStackTrace();
        }
    }
}
