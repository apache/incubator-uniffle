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

package org.apache.hadoop.mapreduce;

import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapred.JobConf;

import org.apache.uniffle.common.config.ConfigOption;
import org.apache.uniffle.common.config.ConfigOptions;
import org.apache.uniffle.common.config.ConfigUtils;
import org.apache.uniffle.common.config.RssClientConf;

public class RssMRClientConf extends RssClientConf {
  public static final String MR_RSS_CONFIG_PREFIX = "mapreduce.";
  public static final String RSS_ASSIGNMENT_PREFIX = MR_RSS_CONFIG_PREFIX + "rss.assignment.partition.";
  public static final long RSS_WRITER_BUFFER_SIZE_DEFAULT_VALUE = 1024 * 1024 * 14;
  public static final String RSS_CONF_FILE = "rss_conf.xml";
  public static final Set<String> RSS_MANDATORY_CLUSTER_CONF =
      ImmutableSet.of(
          RSS_STORAGE_TYPE.key(),
          RSS_REMOTE_STORAGE_PATH.key()
      );

  public static final ConfigOption<Float> RSS_CLIENT_SEND_THRESHOLD = ConfigOptions
      .key("rss.client.send.threshold")
      .floatType()
      .defaultValue(0.2f);

  public static final ConfigOption<Integer> RSS_CLIENT_BATCH_TRIGGER_NUM = ConfigOptions
      .key("rss.client.batch.trigger.num")
      .intType()
      .defaultValue(50);

  public static final ConfigOption<Float> RSS_CLIENT_SORT_MEMORY_USE_THRESHOLD = ConfigOptions
      .key("rss.client.sort.memory.use.threshold")
      .floatType()
      .defaultValue(0.9f);

  public static final ConfigOption<Float> RSS_CLIENT_MEMORY_THRESHOLD = ConfigOptions
      .key("rss.client.memory.threshold")
      .floatType()
      .defaultValue(0.8f);

  public static final ConfigOption<Integer> RSS_CLIENT_BITMAP_NUM = ConfigOptions
      .key("rss.client.bitmap.num")
      .intType()
      .defaultValue(1);

  public static final ConfigOption<Long> RSS_CLIENT_MAX_SEGMENT_SIZE = ConfigOptions
      .key("rss.client.max.buffer.size")
      .longType()
      .defaultValue(3 * 1024L);

  public static final ConfigOption<Boolean> RSS_REDUCE_REMOTE_SPILL_ENABLED = ConfigOptions
      .key("rss.reduce.remote.spill.enable")
      .booleanType()
      .defaultValue(false);

  public static final ConfigOption<Integer> RSS_REDUCE_REMOTE_SPILL_ATTEMPT_INC = ConfigOptions
      .key("rss.reduce.remote.spill.attempt.inc")
      .intType()
      .defaultValue(1);

  public static final ConfigOption<Integer> RSS_REDUCE_REMOTE_SPILL_REPLICATION = ConfigOptions
      .key("rss.reduce.remote.spill.replication")
      .intType()
      .defaultValue(1);

  public static final ConfigOption<Integer> RSS_REDUCE_REMOTE_SPILL_RETRIES = ConfigOptions
      .key("rss.reduce.remote.spill.retries")
      .intType()
      .defaultValue(5);

  public static final ConfigOption<String> RSS_REMOTE_STORAGE_CONF = ConfigOptions
      .key("rss.remote.storage.conf")
      .stringType()
      .noDefaultValue();

  private RssMRClientConf(JobConf jobConf) {
    List<ConfigOption<Object>> configOptions = ConfigUtils.getAllConfigOptions(RssMRClientConf.class);

    for (ConfigOption<Object> option : configOptions) {
      String val = jobConf.get(MR_RSS_CONFIG_PREFIX + option.key());
      if (StringUtils.isNotEmpty(val)) {
        set(option, ConfigUtils.convertValue(val, option.getClazz()));
      }
    }
  }

  public static RssMRClientConf from(JobConf jobConf) {
    return new RssMRClientConf(jobConf);
  }
}
