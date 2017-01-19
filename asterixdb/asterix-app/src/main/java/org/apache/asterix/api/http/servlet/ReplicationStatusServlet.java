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
package org.apache.asterix.api.http.servlet;

import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.replication.management.ReplicationChannel;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.logging.Logger;

public class ReplicationStatusServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(ReplicationStatusServlet.class.getName());
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        PrintWriter out = response.getWriter();
//        if (AsterixAppContextInfo.INSTANCE.initialized()
//                && AsterixAppContextInfo.INSTANCE.getCCApplicationContext() != null) {
//            Map<String, ClusterPartition[]> nodePartitions = AsterixAppContextInfo.INSTANCE.getMetadataProperties()
//                    .getNodePartitions();
//            nodePartitions.entrySet().stream().forEach(e -> out.write("Node: " + e.getKey() + " Partitions: " + e
//                    .getValue()));
//
//        }
        response.setContentType("application/json");
        response.setCharacterEncoding("utf-8");
        //String opStatus = ReplicationChannel.printOpStatus();

        //LOGGER.info("REPLICATION OP-STATUS: " + opStatus);
        //out.write(opStatus);
        out.write("Not Implemented");
        response.setStatus(HttpServletResponse.SC_OK);
        out.flush();
    }
}