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

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class YamlClientConfParserTest {

  @Test
  public void testFromFile() throws Exception {
    ClientConfParser parser = new YamlClientConfParser();
    ClientConf conf =
        parser.tryParse(
            getClass().getClassLoader().getResource("dynamicClientConf.yaml").openStream());
    assertEquals("v1", conf.getRssClientConf().get("k1"));
    assertEquals("v2", conf.getRssClientConf().get("k2"));
    assertEquals("true", conf.getRssClientConf().get("k3"));
    assertEquals("false", conf.getRssClientConf().get("k4"));
    assertEquals("1", conf.getRssClientConf().get("k5"));
    assertEquals("2", conf.getRssClientConf().get("k6"));

    assertEquals(
        "v1,v2,v3", conf.getRemoteStorageInfos().get("hdfs://a-ns01").getConfItems().get("k1"));
    assertEquals("v1", conf.getRemoteStorageInfos().get("hdfs://x-ns01").getConfItems().get("k1"));
  }

  @Test
  public void testParse() throws Exception {
    ClientConfParser parser = new YamlClientConfParser();

    // rssClientConf with format of 'k : v'

    String yaml = "rssClientConf:\n    k1: v1\n    k2: v2";

    ClientConf conf = parser.tryParse(IOUtils.toInputStream(yaml));
    assertEquals(2, conf.getRssClientConf().size());
    assertEquals("v1", conf.getRssClientConf().get("k1"));
    assertEquals("v2", conf.getRssClientConf().get("k2"));

    // rssClientConf with format of xml

    yaml =
        "rssClientConf: |+\n"
            + "   \t<configuration>\n"
            + "    <property>\n"
            + "      <name>k1</name>\n"
            + "      <value>v1</value>\n"
            + "    </property>\n"
            + "    <property>\n"
            + "      <name>k2</name>\n"
            + "      <value>v2</value>\n"
            + "    </property>\n"
            + "    </configuration>";

    conf = parser.tryParse(IOUtils.toInputStream(yaml));
    assertEquals(2, conf.getRssClientConf().size());
    assertEquals("v1", conf.getRssClientConf().get("k1"));
    assertEquals("v2", conf.getRssClientConf().get("k2"));

    // remote storage conf with the format of "k : v"

    yaml =
        "remoteStorageInfos:\n"
            + "   hdfs://a-ns01:\n"
            + "      k1: v1,v5\n"
            + "      k2: v2\n"
            + "   hdfs://x-ns01:\n"
            + "      k1: v1\n"
            + "      k2: v2";
    conf = parser.tryParse(IOUtils.toInputStream(yaml));
    assertEquals(0, conf.getRssClientConf().size());
    assertEquals(2, conf.getRemoteStorageInfos().size());
    assertEquals(
        "v1,v5", conf.getRemoteStorageInfos().get("hdfs://a-ns01").getConfItems().get("k1"));
    assertEquals("v1", conf.getRemoteStorageInfos().get("hdfs://x-ns01").getConfItems().get("k1"));

    yaml =
        "remoteStorageInfos:\n"
            + "   hdfs://a-ns01: |+\n"
            + "      <configuration>\n"
            + "      <property>\n"
            + "        <name>k1</name>\n"
            + "        <value>v1</value>\n"
            + "      </property>\n"
            + "      <property>\n"
            + "        <name>k2</name>\n"
            + "        <value>v2</value>\n"
            + "      </property>\n"
            + "      </configuration>\n"
            + "      \n"
            + "   hdfs://x-ns01:\n"
            + "      k1: v1\n"
            + "      k2: v2\n"
            + "\n";
    conf = parser.tryParse(IOUtils.toInputStream(yaml));
    assertEquals(0, conf.getRssClientConf().size());
    assertEquals(2, conf.getRemoteStorageInfos().size());
    assertEquals("v1", conf.getRemoteStorageInfos().get("hdfs://a-ns01").getConfItems().get("k1"));
    assertEquals("v1", conf.getRemoteStorageInfos().get("hdfs://x-ns01").getConfItems().get("k1"));
  }
}
