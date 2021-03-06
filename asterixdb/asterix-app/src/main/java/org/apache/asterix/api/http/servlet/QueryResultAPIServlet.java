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

import static org.apache.asterix.api.http.servlet.ServletConstants.HYRACKS_CONNECTION_ATTR;
import static org.apache.asterix.api.http.servlet.ServletConstants.HYRACKS_DATASET_ATTR;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.app.result.ResultUtil;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.asterix.translator.SessionConfig;
import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.client.dataset.HyracksDataset;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class QueryResultAPIServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(QueryResultAPIServlet.class.getName());

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        int respCode = HttpServletResponse.SC_OK;
        response.setContentType("text/html"); // TODO this seems wrong ...
        response.setCharacterEncoding("utf-8");
        String strHandle = request.getParameter("handle");
        PrintWriter out = response.getWriter();
        ServletContext context = getServletContext();
        IHyracksClientConnection hcc;
        IHyracksDataset hds;

        try {
            if (strHandle == null || strHandle.isEmpty()) {
                throw new AsterixException("Empty request, no handle provided");
            }

            HyracksProperties hp = new HyracksProperties();
            String strIP = hp.getHyracksIPAddress();
            int port = hp.getHyracksPort();

            synchronized (context) {
                hcc = (IHyracksClientConnection) context.getAttribute(HYRACKS_CONNECTION_ATTR);
                if (hcc == null) {
                    hcc = new HyracksConnection(strIP, port);
                    context.setAttribute(HYRACKS_CONNECTION_ATTR, hcc);
                }

                hds = (IHyracksDataset) context.getAttribute(HYRACKS_DATASET_ATTR);
                if (hds == null) {
                    hds = new HyracksDataset(hcc, ResultReader.FRAME_SIZE, ResultReader.NUM_READERS);
                    context.setAttribute(HYRACKS_DATASET_ATTR, hds);
                }
            }
            ObjectMapper om = new ObjectMapper();
            ObjectNode handleObj = (ObjectNode) om.readTree(strHandle);
            ArrayNode handle = (ArrayNode) handleObj.get("handle");
            JobId jobId = new JobId(handle.get(0).asLong());
            ResultSetId rsId = new ResultSetId(handle.get(1).asLong());

            ResultReader resultReader = new ResultReader(hds);
            resultReader.open(jobId, rsId);

            // QQQ The output format is determined by the initial
            // query and cannot be modified here, so calling back to
            // initResponse() is really an error. We need to find a
            // way to send the same OutputFormat value here as was
            // originally determined there. Need to save this value on
            // some object that we can obtain here.
            SessionConfig sessionConfig = RESTAPIServlet.initResponse(request, response);
            ResultUtil.printResults(resultReader, sessionConfig, new Stats(), null);

        } catch (Exception e) {
            respCode = HttpServletResponse.SC_BAD_REQUEST;
            out.println(e.getMessage());
            LOGGER.log(Level.WARNING, "Error retrieving result", e);
        }
        response.setStatus(respCode);
        if (out.checkError()) {
            LOGGER.warning("Error flushing output writer");
        }
    }
}
