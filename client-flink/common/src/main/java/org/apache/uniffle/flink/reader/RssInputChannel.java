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

package org.apache.uniffle.flink.reader;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.api.ShuffleReadClient;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.flink.resource.DefaultRssShuffleResource;
import org.apache.uniffle.flink.resource.RssShuffleResourceDescriptor;
import org.apache.uniffle.flink.shuffle.RssShuffleDescriptor;

public class RssInputChannel {
  private final ShuffleDescriptor shuffleDescriptor;
  private final int channelIndex;
  private final ShuffleWriteClient shuffleWriteClient;
  private ShuffleReadClient shuffleReadClient;
  private final String clientType;
  private final InputChannelInfo inputChannelInfo;

  public RssInputChannel(
      ShuffleDescriptor shuffleDescriptor,
      int channelIndex,
      ShuffleWriteClient shuffleWriteClient,
      String clientType,
      InputChannelInfo inputChannelInfo) {
    this.shuffleDescriptor = shuffleDescriptor;
    this.channelIndex = channelIndex;
    this.shuffleWriteClient = shuffleWriteClient;
    this.clientType = clientType;
    this.inputChannelInfo = inputChannelInfo;
  }

  public int getChannelIndex() {
    return channelIndex;
  }

  public ShuffleReadClient getShuffleReadClient(
      String basePath,
      org.apache.hadoop.conf.Configuration configuration,
      ShuffleDataDistributionType dataDistributionType,
      RssConf rssConf) {

    if (shuffleReadClient != null) {
      return shuffleReadClient;
    }

    RssShuffleDescriptor rssShuffleDescriptor = (RssShuffleDescriptor) shuffleDescriptor;
    DefaultRssShuffleResource defaultRssShuffleResource = rssShuffleDescriptor.getShuffleResource();
    RssShuffleResourceDescriptor rssShuffleResourceDescriptor =
        defaultRssShuffleResource.getShuffleResourceDescriptor();

    String appId = rssShuffleDescriptor.getJobId().toString();
    int shuffleId = rssShuffleResourceDescriptor.getShuffleId();
    int attemptId = rssShuffleResourceDescriptor.getAttemptId();
    int partitionId = rssShuffleResourceDescriptor.getPartitionId();
    int partitionNum = rssShuffleResourceDescriptor.getMapPartitionId();

    List<ShuffleServerInfo> shuffleServers =
        defaultRssShuffleResource.getPartitionLocation(partitionNum);

    Roaring64NavigableMap blockMaps =
        genPartitionToExpectBlocks(clientType, shuffleServers, appId, shuffleId, partitionId);
    Roaring64NavigableMap taskMaps = genTaskIdBitmap(attemptId);

    boolean expectedTaskIdsBitmapFilterEnable = (shuffleServers.size() > 1);
    shuffleReadClient =
        ShuffleClientFactory.getInstance()
            .createShuffleReadClient(
                ShuffleClientFactory.newReadBuilder()
                    .appId(appId)
                    .shuffleId(shuffleId)
                    .partitionId(partitionId)
                    .basePath(basePath)
                    .partitionNumPerRange(1)
                    .partitionNum(partitionNum)
                    .blockIdBitmap(blockMaps)
                    .taskIdBitmap(taskMaps)
                    .shuffleServerInfoList(shuffleServers)
                    .hadoopConf(configuration)
                    .shuffleDataDistributionType(dataDistributionType)
                    .expectedTaskIdsBitmapFilterEnable(expectedTaskIdsBitmapFilterEnable)
                    .rssConf(rssConf));
    return shuffleReadClient;
  }

  private Roaring64NavigableMap genTaskIdBitmap(int attemptId) {
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf();
    taskIdBitmap.addLong(attemptId);
    return taskIdBitmap;
  }

  private Roaring64NavigableMap genPartitionToExpectBlocks(
      String clientType,
      List<ShuffleServerInfo> shuffleServerList,
      String appId,
      int shuffleId,
      int partitionId) {
    Set<ShuffleServerInfo> shuffleServers = new HashSet<>(shuffleServerList);
    return shuffleWriteClient.getShuffleResult(
        clientType, shuffleServers, appId, shuffleId, partitionId);
  }

  public InputChannelInfo getInputChannelInfo() {
    return inputChannelInfo;
  }

  public void close() {
    if (shuffleReadClient != null) {
      shuffleReadClient.close();
    }
    shuffleReadClient = null;
  }
}
