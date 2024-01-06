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

package org.apache.uniffle.coordinator.conf;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LegacyClientConfParserTest {

  @Test
  public void testParse() throws Exception {
    ClientConfParser parser = new LegacyClientConfParser();
    ClientConf conf =
        parser.tryParse(
            getClass().getClassLoader().getResource("dynamicClientConf.legacy").openStream());
    assertEquals("v1", conf.getRssClientConf().get("k1"));
    assertEquals("v2", conf.getRssClientConf().get("k2"));
    assertEquals("v1", conf.getRemoteStorageInfos().get("hdfs://a-ns01").getConfItems().get("k1"));
    assertEquals(
        "v1,v2,v3", conf.getRemoteStorageInfos().get("hdfs://x-ns01").getConfItems().get("k1"));
    assertEquals("v4", conf.getRemoteStorageInfos().get("hdfs://x-ns01").getConfItems().get("k2"));
    assertEquals(
        "v5,v6", conf.getRemoteStorageInfos().get("hdfs://x-ns01").getConfItems().get("k3"));
  }
}
