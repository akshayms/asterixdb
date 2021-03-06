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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.asterix.runtime.util.AppContextInfo;

import static org.apache.asterix.api.http.servlet.ServletConstants.ASTERIX_BUILD_PROP_ATTR;

public class VersionAPIServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        ServletContext context = getServletContext();
        AppContextInfo props = (AppContextInfo) context.getAttribute(ASTERIX_BUILD_PROP_ATTR);
        Map<String, String> buildProperties = props.getBuildProperties().getAllProps();
        ObjectMapper om = new ObjectMapper();
        ObjectNode responseObject = om.createObjectNode();
        for (Map.Entry<String, String> e : buildProperties.entrySet()) {
            responseObject.put(e.getKey(), e.getValue());
        }
        response.setCharacterEncoding("utf-8");
        PrintWriter responseWriter = response.getWriter();
        responseWriter.write(responseObject.toString());
        response.setStatus(HttpServletResponse.SC_OK);
        responseWriter.flush();
    }
}
