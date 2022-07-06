/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.uniffle.test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.uniffle.client.factory.CoordinatorClientFactory;
import org.apache.uniffle.client.impl.grpc.CoordinatorGrpcClient;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.proto.RssProtos.GetShuffleAssignmentsResponse;
import org.apache.uniffle.proto.RssProtos.StatusCode;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.util.StorageType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.io.Files;
import com.google.common.util.concurrent.Uninterruptibles;

public class ShuffleServerApiTest extends IntegrationTestBase {

    private static Long EVENT_THRESHOLD_SIZE = 2048L;

    private static int HTTP_PORT = 0;
    private static int HTTP_PORT_DEFAULT = 18080;
    private static String SHUFFLE_SERVER_API_BASE_URI = null;
    private static CoordinatorGrpcClient coordinatorClient = null;
    @BeforeAll
    public static void setupServers() throws Exception {
        CoordinatorConf coordinatorConf = getCoordinatorConf();
        coordinatorConf.setLong(CoordinatorConf.COORDINATOR_APP_EXPIRED, 2000);
        createCoordinatorServer(coordinatorConf);
        ShuffleServerConf shuffleServerConf = getShuffleServerConf();
        File tmpDir = Files.createTempDir();
        File dataDir1 = new File(tmpDir, "data1");
        String basePath = dataDir1.getAbsolutePath();
        shuffleServerConf.set(ShuffleServerConf.RSS_STORAGE_TYPE, StorageType.MEMORY_LOCALFILE_HDFS.name());
        shuffleServerConf.set(ShuffleServerConf.FLUSH_COLD_STORAGE_THRESHOLD_SIZE, EVENT_THRESHOLD_SIZE);
        shuffleServerConf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, basePath);
        shuffleServerConf.set(RssBaseConf.RPC_METRICS_ENABLED, true);
        shuffleServerConf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 2000L);
        shuffleServerConf.set(ShuffleServerConf.SERVER_PRE_ALLOCATION_EXPIRED, 5000L);
        shuffleServerConf.set(ShuffleServerConf.SHUFFLE_SERVER_OFFLINE_CHECK_DELAY, 3000L);
        shuffleServerConf.set(ShuffleServerConf.SHUFFLE_SERVER_OFFLINE_CHECK_INTERVAL, 1000L);
        shuffleServerConf.set(ShuffleServerConf.SHUFFLE_SERVER_OFFLINE_CHECK_MAX_TIME, 10000L);
        HTTP_PORT = shuffleServerConf.getInteger("rss.jetty.http.port", 0);
        if (HTTP_PORT == 0) {
            shuffleServerConf.setInteger("rss.jetty.http.port", HTTP_PORT_DEFAULT);
        }
        createShuffleServer(shuffleServerConf);
        startServers();
        SHUFFLE_SERVER_API_BASE_URI = "http://localhost:" + HTTP_PORT + "/server";
        coordinatorClient = (CoordinatorGrpcClient) new CoordinatorClientFactory("GRPC")
                .createCoordinatorClient(LOCALHOST, COORDINATOR_PORT_1);
    }

    @Test
    public void offlineTest() {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            // healthy
            HttpGet isHealthyGet = new HttpGet(SHUFFLE_SERVER_API_BASE_URI + "/isHealthy");
            CloseableHttpResponse response1 = client.execute(isHealthyGet);
            assertTrue(response1.getStatusLine().getStatusCode() == 200);
            String entity1 = EntityUtils.toString(response1.getEntity());
            assertEquals("true", entity1);

            // get shuffle assignments
            GetShuffleAssignmentsResponse assignmentsResponse =
                    coordinatorClient.doGetShuffleAssignments("appId1", 0, 1, 1, 1, Collections.emptySet());
            assertTrue(assignmentsResponse.getStatus() == StatusCode.SUCCESS);

            // offline
            HttpGet offlineGet = new HttpGet(SHUFFLE_SERVER_API_BASE_URI + "/offline");
            CloseableHttpResponse response2 = client.execute(offlineGet);
            assertTrue(response2.getStatusLine().getStatusCode() == 200);

            // unhealthy
            CloseableHttpResponse response3 = client.execute(isHealthyGet);
            assertTrue(response3.getStatusLine().getStatusCode() == 200);
            String entity3 = EntityUtils.toString(response3.getEntity());
            assertEquals("false", entity3);

            // failed to get shuffle assignments
            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
            GetShuffleAssignmentsResponse assignmentsResponse2 =
                    coordinatorClient.doGetShuffleAssignments("appId2", 0, 1, 1, 1, Collections.emptySet());
            assertTrue(assignmentsResponse2.getStatus() == StatusCode.INTERNAL_ERROR);
        } catch (IOException e) {
            fail("offline test failed.", e);
        }
    }

}
