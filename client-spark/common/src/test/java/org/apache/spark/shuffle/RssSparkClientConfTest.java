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

import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.config.RssConf;

import static org.apache.spark.shuffle.RssSparkClientConf.RSS_WRITER_SERIALIZER_BUFFER_SIZE;
import static org.apache.uniffle.common.config.RssClientConf.RSS_CLIENT_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RssSparkClientConfTest {

  @Test
  public void testInitializedFromSparkConf() {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.rss.writer.serializer.buffer.size", "6k");
    sparkConf.set("spark.rss.client.type", "NETTY");

    RssConf rssConf = RssSparkClientConf.from(sparkConf);
    assertEquals("6k", rssConf.get(RSS_WRITER_SERIALIZER_BUFFER_SIZE));
    assertEquals("NETTY", rssConf.get(RSS_CLIENT_TYPE));
  }
}
