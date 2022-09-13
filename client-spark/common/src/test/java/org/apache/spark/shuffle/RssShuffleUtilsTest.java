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

package org.apache.spark.shuffle;

import java.util.Map;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.client.util.RssShuffleUtils;
import org.apache.uniffle.common.config.ConfigOption;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RssShuffleUtilsTest {

  @Test
  public void applyDynamicClientConfTest() {
    final RssClientConf conf = new RssClientConf();

    Map<String, String> clientConf = Maps.newHashMap();

    String remoteStoragePath = "hdfs://path1";
    String mockKey = "spark.mockKey";
    String mockValue = "v";

    clientConf.put(RssSparkClientConf.RSS_REMOTE_STORAGE_PATH.key(), remoteStoragePath);
    clientConf.put(RssSparkClientConf.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE_HDFS.name());
    clientConf.put(mockKey, mockValue);

    putWithDefaultVal(clientConf, RssSparkClientConf.RSS_CLIENT_TYPE);
    putWithDefaultVal(clientConf, RssSparkClientConf.RSS_CLIENT_RETRY_MAX);
    putWithDefaultVal(clientConf, RssSparkClientConf.RSS_CLIENT_RETRY_INTERVAL_MAX);
    putWithDefaultVal(clientConf, RssSparkClientConf.RSS_DATA_REPLICA);
    putWithDefaultVal(clientConf, RssSparkClientConf.RSS_DATA_REPLICA_WRITE);
    putWithDefaultVal(clientConf, RssSparkClientConf.RSS_CLIENT_READ_BUFFER_SIZE);
    putWithDefaultVal(clientConf, RssSparkClientConf.RSS_INDEX_READ_LIMIT);
    putWithDefaultVal(clientConf, RssSparkClientConf.RSS_PARTITION_NUM_PER_RANGE);
    putWithDefaultVal(clientConf, RssSparkClientConf.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS);
    putWithDefaultVal(clientConf, RssSparkClientConf.RSS_CLIENT_SEND_CHECK_INTERVAL_MS);
    putWithDefaultVal(clientConf, RssSparkClientConf.RSS_HEARTBEAT_INTERVAL);
    putWithDefaultVal(clientConf, RssSparkClientConf.RSS_DATA_REPLICA_READ);

    RssShuffleUtils.applyDynamicClientConf(RssSparkClientConf.RSS_MANDATORY_CLUSTER_CONF, conf, clientConf);

    assertEquals(remoteStoragePath, conf.get(RssSparkClientConf.RSS_REMOTE_STORAGE_PATH));
    equalsWithDefaultVal(conf, RssSparkClientConf.RSS_CLIENT_TYPE);
    equalsWithDefaultVal(conf, RssSparkClientConf.RSS_CLIENT_RETRY_MAX);
    equalsWithDefaultVal(conf, RssSparkClientConf.RSS_INDEX_READ_LIMIT);
    equalsWithDefaultVal(conf, RssSparkClientConf.RSS_PARTITION_NUM_PER_RANGE);
    equalsWithDefaultVal(conf, RssSparkClientConf.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS);
    equalsWithDefaultVal(conf, RssSparkClientConf.RSS_CLIENT_SEND_CHECK_INTERVAL_MS);
    equalsWithDefaultVal(conf, RssSparkClientConf.RSS_HEARTBEAT_INTERVAL);
    equalsWithDefaultVal(conf, RssSparkClientConf.RSS_DATA_REPLICA_READ);
    equalsWithDefaultVal(conf, RssSparkClientConf.RSS_DATA_REPLICA_WRITE);
    equalsWithDefaultVal(conf, RssSparkClientConf.RSS_DATA_REPLICA);
    equalsWithDefaultVal(conf, RssSparkClientConf.RSS_CLIENT_RETRY_INTERVAL_MAX);

    assertEquals(StorageType.MEMORY_LOCALFILE_HDFS.name(), conf.get(RssSparkClientConf.RSS_STORAGE_TYPE));

    assertEquals(mockValue, conf.getString(mockKey, ""));

    String remoteStoragePath2 = "hdfs://path2";
    clientConf = Maps.newHashMap();
    clientConf.put(RssSparkClientConf.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_HDFS.name());
    clientConf.put(RssSparkClientConf.RSS_REMOTE_STORAGE_PATH.key(), remoteStoragePath2);
    clientConf.put(mockKey, "won't be rewrite");
    clientConf.put(RssSparkClientConf.RSS_CLIENT_RETRY_MAX.key(), "99999");
    RssShuffleUtils.applyDynamicClientConf(RssSparkClientConf.RSS_MANDATORY_CLUSTER_CONF, conf, clientConf);
    // overwrite
    assertEquals(remoteStoragePath2, conf.get(RssSparkClientConf.RSS_REMOTE_STORAGE_PATH));
    assertEquals(StorageType.MEMORY_HDFS.name(), conf.get(RssSparkClientConf.RSS_STORAGE_TYPE));
    // won't be overwrite
    assertEquals(mockValue, conf.getString(mockKey, ""));
    assertEquals(
        RssSparkClientConf.RSS_CLIENT_RETRY_MAX.defaultValue(),
        conf.get(RssSparkClientConf.RSS_CLIENT_RETRY_MAX)
    );
  }

  private <T> void equalsWithDefaultVal(RssClientConf conf, ConfigOption<T> option) {
    assertEquals(option.defaultValue(), conf.get(option));
  }

  private <T> void putWithDefaultVal(Map<String, String> clientConf, ConfigOption<T> option) {
    clientConf.put(option.key(), String.valueOf(option.defaultValue()));
  }
}
