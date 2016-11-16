package org.apache.asterix.api.http.servlet;

import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.runtime.util.AsterixAppContextInfo;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

/**
 * Created by msa on 11/15/16.
 */
public class ReplicationStatusServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        PrintWriter out = response.getWriter();
        if (AsterixAppContextInfo.INSTANCE.initialized()
                && AsterixAppContextInfo.INSTANCE.getCCApplicationContext() != null) {
            Map<String, ClusterPartition[]> nodePartitions = AsterixAppContextInfo.INSTANCE.getMetadataProperties()
                    .getNodePartitions();
            nodePartitions.entrySet().stream().forEach(e -> out.write("Node: " + e.getKey() + " Partitions: " + e
                    .getValue()));
        }

    }
}
