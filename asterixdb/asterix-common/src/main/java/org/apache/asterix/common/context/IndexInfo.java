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
package org.apache.asterix.common.context;

import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;

public class IndexInfo extends Info {
    private final ILSMIndex index;
    private final long resourceId;
    private final int datasetId;
    private boolean isActivePartitionIndex;

    public IndexInfo(ILSMIndex index, int datasetId, long resourceId, boolean isActivePartitionIndex) {
        this.index = index;
        this.datasetId = datasetId;
        this.resourceId = resourceId;
        this.isActivePartitionIndex = isActivePartitionIndex;
    }

    public ILSMIndex getIndex() {
        return index;
    }

    public long getResourceId() {
        return resourceId;
    }

    public int getDatasetId() {
        return datasetId;
    }

    public boolean isActivePartitionIndex() {
        return isActivePartitionIndex;
    }
}
