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

package org.apache.uniffle.common.netty.client;

import java.io.IOException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.config.RssBaseConf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TransportClientFactoryTest extends TransportClientTestBase {

  private static int SERVER_PORT_RANGE_START = 10000;
  private static int SERVER_PORT_RANGE_END = 10005;

  @BeforeAll
  public static void setupServer() {
    for (int i = SERVER_PORT_RANGE_START; i < SERVER_PORT_RANGE_END + 1; i++) {
      mockServers.add(new MockServer(i));
    }
    startMockServer();
  }

  @Test
  public void testCreateClient() throws IOException, InterruptedException {
    RssBaseConf rssBaseConf = new RssBaseConf();
    rssBaseConf.setInteger("rss.client.netty.client.connections.per.peer", 1);
    TransportConf transportConf = new TransportConf(rssBaseConf);
    TransportContext transportContext = new TransportContext(transportConf);
    TransportClient transportClient1 =
        transportContext
            .createClientFactory()
            .createClient("localhost", SERVER_PORT_RANGE_START, 1);
    assertTrue(transportClient1.isActive());
    transportClient1.close();

    TransportClient transportClient2 =
        transportContext
            .createClientFactory()
            .createClient("localhost", SERVER_PORT_RANGE_START, 1);
    assertNotEquals(transportClient1, transportClient2);
    assertTrue(transportClient2.isActive());
  }

  @Test
  public void testClientReuse() throws IOException, InterruptedException {
    RssBaseConf rssBaseConf = new RssBaseConf();
    TransportConf transportConf = new TransportConf(rssBaseConf);
    TransportContext transportContext = new TransportContext(transportConf);
    TransportClientFactory transportClientFactory = transportContext.createClientFactory();
    TransportClient client1 =
        transportClientFactory.createClient("localhost", SERVER_PORT_RANGE_START, 1);
    TransportClient client2 =
        transportClientFactory.createClient("localhost", SERVER_PORT_RANGE_START, 1);
    assertEquals(client1, client2);
  }

  @Test
  public void testClientDiffPartition() throws IOException, InterruptedException {
    RssBaseConf rssBaseConf = new RssBaseConf();
    rssBaseConf.setInteger("rss.client.netty.client.connections.per.peer", 1);
    TransportConf transportConf = new TransportConf(rssBaseConf);
    TransportContext transportContext = new TransportContext(transportConf);
    TransportClientFactory transportClientFactory = transportContext.createClientFactory();
    TransportClient client1 =
        transportClientFactory.createClient("localhost", SERVER_PORT_RANGE_START, 1);
    TransportClient client2 =
        transportClientFactory.createClient("localhost", SERVER_PORT_RANGE_START, 2);
    assertEquals(client1, client2);
    transportClientFactory.close();

    rssBaseConf.setInteger("rss.client.netty.client.connections.per.peer", 10);
    transportConf = new TransportConf(rssBaseConf);
    transportContext = new TransportContext(transportConf);
    transportClientFactory = transportContext.createClientFactory();
    client1 = transportClientFactory.createClient("localhost", SERVER_PORT_RANGE_START, 1);
    client2 = transportClientFactory.createClient("localhost", SERVER_PORT_RANGE_START, 2);
    assertNotEquals(client1, client2);
    transportClientFactory.close();
  }

  @Test
  public void testClientDiffServer() throws IOException, InterruptedException {
    RssBaseConf rssBaseConf = new RssBaseConf();
    rssBaseConf.setInteger("rss.client.netty.client.connections.per.peer", 1);
    TransportConf transportConf = new TransportConf(rssBaseConf);
    TransportContext transportContext = new TransportContext(transportConf);
    TransportClientFactory transportClientFactory = transportContext.createClientFactory();
    TransportClient client1 =
        transportClientFactory.createClient("localhost", SERVER_PORT_RANGE_START, 1);
    TransportClient client2 =
        transportClientFactory.createClient("localhost", SERVER_PORT_RANGE_START + 1, 1);
    assertNotEquals(client1, client2);
    transportClientFactory.close();
  }
}
