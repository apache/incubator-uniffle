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

package org.apache.tez.common;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.config.RssConf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TezClientConfTest {

  private static TezClientConf tezClientConf;
  private static Configuration config;

  @BeforeAll
  public static void setUp() {
    config = new Configuration();
    tezClientConf = new TezClientConf(config);
  }

  @Test
  public void testTezClientConf() {
    Configuration conf = new Configuration(false);
    conf.set("tez.config1", "value1");
    conf.setInt(TezClientConf.RSS_CLIENT_HEARTBEAT_THREAD_NUM.key(), 100);
    conf.set(TezClientConf.RSS_REMOTE_STORAGE_PATH.key(), "/tmp/rss/");
    conf.setInt(TezClientConf.RSS_CLIENT_RETRY_MAX.key(), 5);
    conf.set(TezClientConf.RSS_STORAGE_TYPE.key(), "MEMORY_LOCALFILE_HDFS");

    TezClientConf tezClientConf = new TezClientConf(conf);
    assertEquals("value1", tezClientConf.getString("tez.config1", null));
    assertEquals(100, tezClientConf.get(TezClientConf.RSS_CLIENT_HEARTBEAT_THREAD_NUM));
    assertEquals("/tmp/rss/", tezClientConf.get(TezClientConf.RSS_REMOTE_STORAGE_PATH));
    assertEquals(5, tezClientConf.get(TezClientConf.RSS_CLIENT_RETRY_MAX));

    RssConf rssConf = tezClientConf.toRssConf();
    assertEquals(
        tezClientConf.get(TezClientConf.RSS_STORAGE_TYPE),
        rssConf.get(RssBaseConf.RSS_STORAGE_TYPE).toString());
    assertEquals(
        tezClientConf.get(TezClientConf.RSS_STORAGE_TYPE),
        TezClientConf.toRssConf(conf).getString(RssBaseConf.RSS_STORAGE_TYPE.key(), null));

    Configuration confNew = tezClientConf.getHadoopConfig();
    assertEquals("value1", confNew.get("tez.config1"));
    assertEquals(100, confNew.getInt(TezClientConf.RSS_CLIENT_HEARTBEAT_THREAD_NUM.key(), -1));
    assertEquals("/tmp/rss/", confNew.get(TezClientConf.RSS_REMOTE_STORAGE_PATH.key()));
    assertEquals(5, confNew.getInt(TezClientConf.RSS_CLIENT_RETRY_MAX.key(), -1));
  }

  @Test
  public void testGetHadoopConfig() {
    assertEquals(config, tezClientConf.getHadoopConfig());
  }

  @Test
  public void testLoadConfFromHadoopConfig() {
    boolean result = tezClientConf.loadConfFromHadoopConfig(config);
    assertTrue(result);
  }

  @Test
  public void testToRssConf() {
    RssConf rssConf = TezClientConf.toRssConf(config);
    assertNotNull(rssConf);
  }
}
