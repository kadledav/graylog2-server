/**
 * This file is part of Graylog2.
 *
 * Graylog2 is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog2 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog2.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.graylog2.radio.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.rest.models.system.radio.requests.PingRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.net.URI;

public class Ping {
    /*
     * This is extremely simple. Once we do more than just the ping API calls
     * we should build something proper here.
     */

    private static final Logger LOG = LoggerFactory.getLogger(Ping.class);
    private static final MediaType CONTENT_TYPE = MediaType.parse("application/json");

    public static void ping(OkHttpClient client, ObjectMapper mapper, URI server, URI ourUri, String radioId) throws IOException {
        final PingRequest pingRequest = PingRequest.create(ourUri.toString());

        final URI uri = server.resolve("/system/radios/" + radioId + "/ping");
        final Request request = new Request.Builder()
                .url(uri.toURL())
                .put(RequestBody.create(CONTENT_TYPE, mapper.writeValueAsBytes(pingRequest)))
                .build();

        final Response r = client.newCall(request).execute();

        // fail on a non-ok status
        if (!r.isSuccessful()) {
            throw new RuntimeException("Expected successful HTTP response [2xx] but got [" + r.code() + "]. Request was " + request.urlString());
        }
    }

    public static class Pinger implements Runnable {

        private final OkHttpClient httpClient;
        private final ObjectMapper objectMapper;
        private final String nodeId;
        private final URI serverUri;
        private final URI ourUri;

        @Inject
        public Pinger(OkHttpClient httpClient,
                      ObjectMapper objectMapper,
                      @Named("rest_transport_uri") URI ourUri,
                      @Named("graylog2_server_uri") URI serverUri,
                      ServerStatus serverStatus) {
            this.httpClient = httpClient;
            this.objectMapper = objectMapper;
            this.nodeId = serverStatus.getNodeId().toString();
            this.ourUri = ourUri;
            this.serverUri = serverUri;
        }

        @Override
        public void run() {
            ping();
        }

        public void ping() {
            LOG.debug("Updating (ping) this radio instance [{}] in the Graylog2 cluster at [{}]", nodeId, serverUri);
            try {
                Ping.ping(httpClient, objectMapper, serverUri, ourUri, nodeId);
            } catch (Exception e) {
                LOG.error("Cluster ping failed.", e);
            }
        }
    }
}
