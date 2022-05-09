/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.ShuffleConsumerPlugin;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapreduce.RssMRConfig;
import org.apache.hadoop.mapreduce.RssMRUtils;
import org.apache.hadoop.util.Progress;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import com.tencent.rss.client.api.ShuffleReadClient;
import com.tencent.rss.client.api.ShuffleWriteClient;
import com.tencent.rss.client.factory.ShuffleClientFactory;
import com.tencent.rss.client.request.CreateShuffleReadClientRequest;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.util.UnitConverter;

public class RssShuffle<K, V> implements ShuffleConsumerPlugin<K, V>, ExceptionReporter {

  private static final Log LOG = LogFactory.getLog(RssShuffle.class);

  private static final int MAX_EVENTS_TO_FETCH = 10000;

  private ShuffleConsumerPlugin.Context context;

  private org.apache.hadoop.mapreduce.TaskAttemptID reduceId;
  private JobConf jobConf;
  private Reporter reporter;
  private ShuffleClientMetrics metrics;
  private TaskUmbilicalProtocol umbilical;

  private MergeManager<K, V> merger;
  private Throwable throwable = null;
  private String throwingThreadName = null;
  private Progress copyPhase;
  private TaskStatus taskStatus;
  private Task reduceTask; //Used for status updates

  private String appId;
  private String storageType;
  private String clientType;
  private int replicaWrite;
  private int replicaRead;
  private int replica;

  private int partitionNum;
  private int partitionNumPerRange;
  private String basePath;
  private int indexReadLimit;
  private int readBufferSize;

  @Override
  public void init(ShuffleConsumerPlugin.Context context) {
    // mapreduce's builtin init
    this.context = context;

    this.reduceId = context.getReduceId();
    this.jobConf = context.getJobConf();
    this.umbilical = context.getUmbilical();
    this.reporter = context.getReporter();
    this.metrics = new ShuffleClientMetrics(reduceId, jobConf);
    this.copyPhase = context.getCopyPhase();
    this.taskStatus = context.getStatus();
    this.reduceTask = context.getReduceTask();
    this.merger = createMergeManager(context);

    // rss init
    this.appId = RssMRUtils.getApplicationAttemptId().toString();
    this.storageType = jobConf.get(RssMRConfig.RSS_STORAGE_TYPE);
    this.replicaWrite = jobConf.getInt(RssMRConfig.RSS_DATA_REPLICA_WRITE,
      RssMRConfig.RSS_DATA_REPLICA_WRITE_DEFAULT_VALUE);
    this.replicaRead = jobConf.getInt(RssMRConfig.RSS_DATA_REPLICA_READ,
      RssMRConfig.RSS_DATA_REPLICA_READ_DEFAULT_VALUE);
    this.replica = jobConf.getInt(RssMRConfig.RSS_DATA_REPLICA,
      RssMRConfig.RSS_DATA_REPLICA_DEFAULT_VALUE);

    this.partitionNum = jobConf.getNumReduceTasks();
    this.partitionNumPerRange = jobConf.getInt(RssMRConfig.RSS_PARTITION_NUM_PER_RANGE,
      RssMRConfig.RSS_PARTITION_NUM_PER_RANGE_DEFAULT_VALUE);
    this.basePath = jobConf.get(RssMRConfig.RSS_REMOTE_STORAGE_PATH);
    this.indexReadLimit = jobConf.getInt(RssMRConfig.RSS_INDEX_READ_LIMIT,
      RssMRConfig.RSS_INDEX_READ_LIMIT_DEFAULT_VALUE);
    this.readBufferSize = (int)UnitConverter.byteStringAsBytes(
      jobConf.get(RssMRConfig.RSS_CLIENT_READ_BUFFER_SIZE,
        RssMRConfig.RSS_CLIENT_READ_BUFFER_SIZE_DEFAULT_VALUE));
   }

  protected MergeManager<K, V> createMergeManager(
    ShuffleConsumerPlugin.Context context) {
    return new MergeManagerImpl<K, V>(reduceId, jobConf, context.getLocalFS(),
      context.getLocalDirAllocator(), reporter, context.getCodec(),
      context.getCombinerClass(), context.getCombineCollector(),
      context.getSpilledRecordsCounter(),
      context.getReduceCombineInputCounter(),
      context.getMergedMapOutputsCounter(), this, context.getMergePhase(),
      context.getMapOutputFile());
  }

  @Override
  public RawKeyValueIterator run() throws IOException, InterruptedException {

    // get assigned RSS servers
    Set<ShuffleServerInfo> serverInfoSet = RssMRUtils.getAssignedServers(jobConf,
        reduceId.getTaskID().getId());
    List<ShuffleServerInfo> serverInfoList = new ArrayList<>();
    for (ShuffleServerInfo server: serverInfoSet) {
      serverInfoList.add(server);
    }

    // just get blockIds from RSS servers
    ShuffleWriteClient writeClient = RssMRUtils.createShuffleClient(jobConf);
    Roaring64NavigableMap blockIdBitmap = writeClient.getShuffleResult(
      clientType, serverInfoSet, appId, 0, reduceId.getTaskID().getId());
    writeClient.close();

    // get map-completion events to generate RSS taskIDs
    final RssEventFetcher<K,V> eventFetcher =
      new RssEventFetcher<K,V>(reduceId, umbilical, jobConf, MAX_EVENTS_TO_FETCH);
    Roaring64NavigableMap taskIdBitmap = eventFetcher.fetchAllRssTaskIds();

    LOG.info("In reduce: " + reduceId
      + ", RSS MR client has fetched blockIds and taskIds successfully");

    // start fetcher to fetch blocks from RSS servers
    if (!taskIdBitmap.isEmpty()) {
      LOG.info("In reduce: " + reduceId
        + ", Rss MR client starts to fetch blocks from RSS server");
      CreateShuffleReadClientRequest request = new CreateShuffleReadClientRequest(
        appId, 0, reduceId.getTaskID().getId(), storageType, basePath, indexReadLimit, readBufferSize,
        partitionNumPerRange, partitionNum, blockIdBitmap, taskIdBitmap, serverInfoList, jobConf);
      ShuffleReadClient shuffleReadClient = ShuffleClientFactory.getInstance().createShuffleReadClient(request);
      RssFetcher fetcher = new RssFetcher(jobConf, reduceId, taskStatus, merger, copyPhase, reporter, metrics,
        shuffleReadClient, blockIdBitmap.getLongCardinality());
      fetcher.fetchAllRssBlocks();
      LOG.info("In reduce: " + reduceId
        + ", Rss MR client fetches blocks from RSS server successfully");
    }

    copyPhase.complete();
    taskStatus.setPhase(TaskStatus.Phase.SORT);
    reduceTask.statusUpdate(umbilical);

    // Finish the on-going merges...
    RawKeyValueIterator kvIter = null;
    try {
      kvIter = merger.close();
    } catch (Throwable e) {
      throw new Shuffle.ShuffleError("Error while doing final merge ", e);
    }

    // Sanity check
    synchronized (this) {
      if (throwable != null) {
        throw new Shuffle.ShuffleError("error in shuffle in " + throwingThreadName,
          throwable);
      }
    }

    LOG.info("In reduce: " + reduceId
      + ", Rss MR client returns sorted data to reduce successfully");

    return kvIter;
  }

  @Override
  public void close() {
  }

  @Override
  public synchronized void reportException(Throwable t) {
    if (throwable == null) {
      throwable = t;
      throwingThreadName = Thread.currentThread().getName();
    }
  }
}
