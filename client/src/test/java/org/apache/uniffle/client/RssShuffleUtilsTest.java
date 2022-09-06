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

package org.apache.uniffle.client;

import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.client.util.RssShuffleUtils;
import org.apache.uniffle.common.config.ConfigOption;
import org.apache.uniffle.common.config.ConfigOptions;
import org.apache.uniffle.common.config.RssConf;

import static org.apache.uniffle.client.RssShuffleUtilsTest.MockedRssClientConf.RSS_CLIENT_RETRY_MAX;
import static org.apache.uniffle.client.RssShuffleUtilsTest.MockedRssClientConf.RSS_CLIENT_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RssShuffleUtilsTest {

  static class MockedRssClientConf extends RssConf {
    public static final ConfigOption<String> RSS_CLIENT_TYPE = ConfigOptions
        .key("rss.client.type")
        .stringType()
        .defaultValue("GRPC")
        .withDescription("");

    public static final ConfigOption<Integer> RSS_CLIENT_RETRY_MAX = ConfigOptions
        .key("rss.client.retry.max")
        .intType()
        .defaultValue(100)
        .withDescription("");
  }

  @Test
  public void applyDynamicClientConfTest() {
    RssConf conf = new MockedRssClientConf();

    Map<String, String> remoteConf = Maps.newHashMap();
    remoteConf.put(RSS_CLIENT_TYPE.key(), "NETTY");
    remoteConf.put(RSS_CLIENT_RETRY_MAX.key(), "200");

    // case1: should be overwritten
    RssShuffleUtils.applyDynamicClientConf(Sets.newHashSet(), conf, remoteConf);
    assertEquals("NETTY", conf.get(RSS_CLIENT_TYPE));
    assertEquals(200, conf.get(RSS_CLIENT_RETRY_MAX));

    // case2: if exist, only key in mandatory list will be overwritten
    conf = new MockedRssClientConf();
    conf.set(RSS_CLIENT_TYPE, "GRPC");
    conf.set(RSS_CLIENT_RETRY_MAX, 300);
    RssShuffleUtils.applyDynamicClientConf(Sets.newHashSet(RSS_CLIENT_TYPE.key()), conf, remoteConf);
    assertEquals("NETTY", conf.get(RSS_CLIENT_TYPE));
    assertEquals(300, conf.get(RSS_CLIENT_RETRY_MAX));

    // case3: if exist, wont be overwritten
    conf = new MockedRssClientConf();
    conf.set(RSS_CLIENT_TYPE, "GRPC");
    conf.set(RSS_CLIENT_RETRY_MAX, 300);
    RssShuffleUtils.applyDynamicClientConf(Sets.newHashSet(), conf, remoteConf);
    assertEquals("GRPC", conf.get(RSS_CLIENT_TYPE));
    assertEquals(300, conf.get(RSS_CLIENT_RETRY_MAX));
  }
}
