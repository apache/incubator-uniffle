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

package org.apache.tez.runtime.library.common.shuffle.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.common.IdUtils;
import org.apache.tez.common.RssTezConfig;
import org.apache.tez.common.RssTezUtils;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.FetchResult;
import org.apache.tez.runtime.library.common.shuffle.FetchedInputAllocator;
import org.apache.tez.runtime.library.common.shuffle.FetcherCallback;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleReadClient;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.request.CreateShuffleReadClientRequest;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.util.UnitConverter;

public class RssTezFetcherTask extends CallableWithNdc<FetchResult> {
  private static final Logger LOG = LoggerFactory.getLogger(RssTezFetcherTask.class);

  private final FetcherCallback fetcherCallback;

  private final InputContext inputContext;
  private final Configuration conf;
  private final FetchedInputAllocator inputManager;
  private final int partition;

  List<InputAttemptIdentifier> inputs;
  private Set<ShuffleServerInfo> serverInfoSet;
  Map<Integer, Roaring64NavigableMap> rssAllBlockIdBitmapMap;
  Map<Integer, Roaring64NavigableMap> rssSuccessBlockIdBitmapMap;
  private String clientType = null;
  private final int numPhysicalInputs;
  private final String appId;
  private final int dagIdentifier;
  private final int vertexIndex;
  private final int reduceId;

  private String storageType;
  private String basePath;
  private final int readBufferSize;
  private final int partitionNumPerRange;
  private final int partitionNum;
  private final int shuffleId;

  public RssTezFetcherTask(FetcherCallback fetcherCallback, InputContext inputContext, Configuration conf,
            FetchedInputAllocator inputManager, int partition, int shuffleId,
            List<InputAttemptIdentifier> inputs, Set<ShuffleServerInfo> serverInfoList,
            Map<Integer, Roaring64NavigableMap> rssAllBlockIdBitmapMap,
            Map<Integer, Roaring64NavigableMap> rssSuccessBlockIdBitmapMap,
            int numPhysicalInputs, int partitionNum) {
    assert (inputs != null && inputs.size() > 0);
    this.fetcherCallback = fetcherCallback;
    this.inputContext = inputContext;
    this.conf = conf;
    this.inputManager = inputManager;
    this.partition = partition;  // partition id to fetch
    this.inputs = inputs;
    this.shuffleId = shuffleId;

    this.serverInfoSet = serverInfoList;
    this.rssAllBlockIdBitmapMap = rssAllBlockIdBitmapMap;
    this.rssSuccessBlockIdBitmapMap = rssSuccessBlockIdBitmapMap;
    this.numPhysicalInputs = numPhysicalInputs;
    this.partitionNum = partitionNum;

    this.appId = IdUtils.getApplicationAttemptId().toString();
    this.dagIdentifier = this.inputContext.getDagIdentifier();
    this.vertexIndex = this.inputContext.getTaskVertexIndex();

    this.reduceId =  this.inputContext.getTaskIndex();
    LOG.info("RssTezFetcherTask, dagIdentifier:{}, vertexIndex:{}, reduceId:{}.", dagIdentifier, vertexIndex, reduceId);
    clientType = conf.get(RssTezConfig.RSS_CLIENT_TYPE, RssTezConfig.RSS_CLIENT_TYPE_DEFAULT_VALUE);
    this.storageType = conf.get(RssTezConfig.RSS_STORAGE_TYPE, RssTezConfig.RSS_STORAGE_TYPE_DEFAULT_VALUE);
    LOG.info("RssTezFetcherTask storageType:{}", storageType);

    String readBufferSize = conf.get(RssTezConfig.RSS_CLIENT_READ_BUFFER_SIZE,
        RssTezConfig.RSS_CLIENT_READ_BUFFER_SIZE_DEFAULT_VALUE);
    this.readBufferSize = (int) UnitConverter.byteStringAsBytes(readBufferSize);
    this.partitionNumPerRange = conf.getInt(RssTezConfig.RSS_PARTITION_NUM_PER_RANGE,
        RssTezConfig.RSS_PARTITION_NUM_PER_RANGE_DEFAULT_VALUE);
    LOG.info("RssTezFetcherTask fetch partition:{}, with inputs:{}, readBufferSize:{}, partitionNumPerRange:{}.",
        this.partition, inputs, this.readBufferSize, this.partitionNumPerRange);
  }

  @Override
  protected FetchResult callInternal() throws Exception {
    ShuffleWriteClient writeClient = RssTezUtils.createShuffleClient(this.conf);
    LOG.info("WriteClient getShuffleResult, clientType:{}, serverInfoSet:{}, appId:{}, shuffleId:{}, partition:{}",
        clientType, serverInfoSet, appId, shuffleId, partition);
    Roaring64NavigableMap blockIdBitmap = writeClient.getShuffleResult(
        clientType, serverInfoSet, appId, shuffleId, partition);
    writeClient.close();
    rssAllBlockIdBitmapMap.put(partition, blockIdBitmap);

    // get map-completion events to generate RSS taskIDs
    // final RssEventFetcher eventFetcher = new RssEventFetcher(inputs, numPhysicalInputs);
    int appAttemptId = IdUtils.getAppAttemptId();
    Roaring64NavigableMap taskIdBitmap = RssTezUtils.fetchAllRssTaskIds(
        new HashSet<>(inputs), numPhysicalInputs,
        appAttemptId);
    LOG.info("inputs:{}, num input:{}, appAttemptId:{}, taskIdBitmap:{}",
        inputs, numPhysicalInputs, appAttemptId, taskIdBitmap);

    LOG.info("In reduce: " + reduceId
        + ", RSS Tez client has fetched blockIds and taskIds successfully");
    // start fetcher to fetch blocks from RSS servers
    if (!taskIdBitmap.isEmpty()) {
      LOG.info("In reduce: " + reduceId + ", Rss Tez client starts to fetch blocks from RSS server");
      JobConf readerJobConf = getRemoteConf();
      LOG.info("RssTezFetcherTask storageType:{}", storageType);
      boolean expectedTaskIdsBitmapFilterEnable = serverInfoSet.size() > 1;
      CreateShuffleReadClientRequest request = new CreateShuffleReadClientRequest(
          appId,
          shuffleId,
          partition,
          basePath,
          partitionNumPerRange,
          partitionNum,
          blockIdBitmap,
          taskIdBitmap,
          new ArrayList<>(serverInfoSet),
          readerJobConf,
          expectedTaskIdsBitmapFilterEnable, RssTezConfig.toRssConf(this.conf));
      ShuffleReadClient shuffleReadClient = ShuffleClientFactory.getInstance().createShuffleReadClient(request);
      RssTezFetcher fetcher = new RssTezFetcher(fetcherCallback,
          inputManager,
          shuffleReadClient,
          rssSuccessBlockIdBitmapMap,
          partition, RssTezConfig.toRssConf(this.conf));
      fetcher.fetchAllRssBlocks();
      LOG.info("In reduce: " + partition
          + ", Rss Tez client fetches blocks from RSS server successfully");
    }
    return null;
  }

  public void shutdown() {
  }

  private JobConf getRemoteConf() {
    return new JobConf(conf);
  }

  public int getPartitionId() {
    return partition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RssTezFetcherTask that = (RssTezFetcherTask) o;
    return partition == that.partition
        && numPhysicalInputs == that.numPhysicalInputs
        && dagIdentifier == that.dagIdentifier
        && vertexIndex == that.vertexIndex
        && reduceId == that.reduceId
        && Objects.equals(appId, that.appId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partition, numPhysicalInputs, dagIdentifier, vertexIndex, reduceId, appId);
  }
}
