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

package org.apache.spark.shuffle.reader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Option;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.shuffle.RssShuffleHandle;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.storage.handler.impl.HadoopShuffleWriteHandler;
import org.apache.uniffle.storage.util.StorageType;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class RssShuffleReaderTest extends AbstractRssReaderTest {

  private static final Serializer KRYO_SERIALIZER = new KryoSerializer(new SparkConf(false));

  @Test
  public void readTest() throws Exception {
    ShuffleServerInfo ssi = new ShuffleServerInfo("127.0.0.1", 0);
    String basePath = HDFS_URI + "readTest1";
    HadoopShuffleWriteHandler writeHandler =
        new HadoopShuffleWriteHandler("appId", 0, 0, 1, basePath, ssi.getId(), conf);

    Map<String, String> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    final Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(writeHandler, 2, 5, expectedData, blockIdBitmap, "key", KRYO_SERIALIZER, 0);

    RssShuffleHandle<String, String, String> handleMock = mock(RssShuffleHandle.class);
    ShuffleDependency<String, String, String> dependencyMock = mock(ShuffleDependency.class);
    when(handleMock.getAppId()).thenReturn("appId");
    when(handleMock.getShuffleId()).thenReturn(1);
    when(handleMock.getDependency()).thenReturn(dependencyMock);
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
    partitionToServers.put(0, Lists.newArrayList(ssi));
    partitionToServers.put(1, Lists.newArrayList(ssi));
    when(handleMock.getPartitionToServers()).thenReturn(partitionToServers);
    when(dependencyMock.serializer()).thenReturn(KRYO_SERIALIZER);
    TaskContext contextMock = mock(TaskContext.class);
    when(contextMock.taskAttemptId()).thenReturn(1L);
    when(contextMock.attemptNumber()).thenReturn(1);
    when(contextMock.taskMetrics()).thenReturn(new TaskMetrics());
    doNothing().when(contextMock).killTaskIfInterrupted();
    when(dependencyMock.mapSideCombine()).thenReturn(false);
    when(dependencyMock.aggregator()).thenReturn(Option.empty());
    when(dependencyMock.keyOrdering()).thenReturn(Option.empty());

    RssConf rssConf = new RssConf();
    rssConf.set(RssClientConf.RSS_STORAGE_TYPE, StorageType.HDFS.name());
    rssConf.set(RssClientConf.RSS_INDEX_READ_LIMIT, 1000);
    rssConf.set(RssClientConf.RSS_CLIENT_READ_BUFFER_SIZE, "1000");
    RssShuffleReader<String, String> rssShuffleReaderSpy =
        spy(
            new RssShuffleReader<>(
                0,
                1,
                contextMock,
                handleMock,
                basePath,
                conf,
                2,
                10,
                blockIdBitmap,
                taskIdBitmap,
                rssConf));

    validateResult(rssShuffleReaderSpy.read(), expectedData, 10);
  }
}
