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

import org.apache.asterix.replication.functions.ReplicationJob;

import java.nio.ByteBuffer;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.logging.LogRecord;

/**
 * Created by msa on 1/17/17.
 */
public class ReplicationJobManager {

    private final Queue replicationQ;
    private final ByteBuffer logPage;

    private static final int DEFAULT_SIZE = 20;
    private static final int DEFAULT_LOG_PAGE_SIZE = 500000;

    public ReplicationJobManager() {
        this.replicationQ = new PriorityQueue(DEFAULT_SIZE);
        this.logPage = ByteBuffer.allocate(DEFAULT_LOG_PAGE_SIZE);
    }

    public void submit(LogRecord logRecord, ReplicationJob replicationJob) {
        // TODO: replicate the logRecord newValue content into a bytebuffer and then push it into the queue;
        replicationQ.add(replicationJob);
    }
}
