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

package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.tez.common.InputContextUtils;
import org.apache.tez.common.RssTezConfig;
import org.apache.tez.common.RssTezUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.UmbilicalUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.shuffle.Record;
import org.apache.uniffle.client.shuffle.reader.KeyValuesReader;
import org.apache.uniffle.client.shuffle.reader.RMRecordsReader;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Private
@InterfaceStability.Unstable
public class RMRssShuffle implements ExceptionReporter {

  private static final Logger LOG = LoggerFactory.getLogger(RMRssShuffle.class);

  private final Configuration conf;
  private final RssConf rssConf;
  private final InputContext inputContext;
  private final int numInputs;
  private final int shuffleId;
  private final ApplicationAttemptId applicationAttemptId;
  private final String appId;
  private ShuffleInputEventHandlerOrderedGrouped eventHandler;
  private final TezTaskAttemptID tezTaskAttemptID;
  private final String srcNameTrimmed;
  private Map<Integer, List<ShuffleServerInfo>> partitionToServers;

  private AtomicBoolean isShutDown = new AtomicBoolean(false);

  final TezCounter skippedInputCounter;
  final TezCounter inputRecordCounter;

  final Map<Integer, Set<InputAttemptIdentifier>> partitionIdToSuccessMapTaskAttempts = new HashMap<>();

  final Set<Integer> partitionIds = new HashSet<>();
  private RMRecordsReader reader = null;
  private RMRssShuffleScheduler scheduler;

  public RMRssShuffle(
      InputContext inputContext,
      Configuration conf,
      int numInputs,
      int shuffleId,
      ApplicationAttemptId applicationAttemptId) throws IOException {
    this.inputContext = inputContext;
    this.conf = conf;
    this.rssConf = RssTezConfig.toRssConf(conf);
    this.numInputs = numInputs;
    this.shuffleId = shuffleId;
    this.applicationAttemptId = applicationAttemptId;
    this.appId = this.applicationAttemptId.toString();
    this.srcNameTrimmed = TezUtilsInternal.cleanVertexName(inputContext.getSourceVertexName());
    LOG.info(srcNameTrimmed + ": Shuffle assigned with " + numInputs + " inputs.");
    this.skippedInputCounter = inputContext.getCounters().findCounter(TaskCounter.NUM_SKIPPED_INPUTS);
    this.inputRecordCounter = inputContext.getCounters().findCounter(TaskCounter.INPUT_RECORDS_PROCESSED);

    this.scheduler = new RMRssShuffleScheduler(this.inputContext, this.conf, numInputs, this, null, null,
        System.currentTimeMillis(), null, false, 0, srcNameTrimmed, this);
    this.eventHandler = new ShuffleInputEventHandlerOrderedGrouped(inputContext, scheduler,
        ShuffleUtils.isTezShuffleHandler(conf));
    this.tezTaskAttemptID = InputContextUtils.getTezTaskAttemptID(this.inputContext);
    // When remote merge is enable, we use the reading-while-processing method, so we set input ready directly.
    inputContext.inputIsReady();
  }

  public void handleEvents(List<Event> events) throws IOException {
    if (!isShutDown.get()) {
      eventHandler.handleEvents(events);
    } else {
      LOG.info(srcNameTrimmed + ": Ignoring events since already shutdown. EventCount: " + events.size());
    }
  }

  public void run() throws IOException {
    this.partitionToServers = UmbilicalUtils.requestShuffleServer(inputContext.getApplicationId(), conf,
        tezTaskAttemptID, shuffleId);
  }

  public void shutdown() {
    if (!isShutDown.getAndSet(true)) {
      if (reader != null) {
        reader.close();
      }
      LOG.info("Shutting down Shuffle for source: " + srcNameTrimmed);
    }
  }

  public void waitForEvents() throws InterruptedException {
    while(!allEventsReceived()) {
      Thread.sleep(100);
    }
    // report unique blocks
    reportUniqueBlockIds();
    if (partitionIds.size() > 0) {
      reader = createRMRecordsReader(partitionIds);
      reader.start();
    }
  }

  private boolean allEventsReceived() {
    return ((scheduler.pathToIdentifierMap.size() + skippedInputCounter.getValue()) == numInputs);
  }

  public void reportUniqueBlockIds() {
    ShuffleWriteClient writeClient = RssTezUtils.createShuffleClient(conf);
    for (int partitionId : partitionIds) {
      Roaring64NavigableMap blockIdBitmap = writeClient.getShuffleResult(null,
          new HashSet<>(partitionToServers.get(partitionId)), appId, shuffleId, partitionId);
      Roaring64NavigableMap taskIdBitmap = RssTezUtils.fetchAllRssTaskIds(
          partitionIdToSuccessMapTaskAttempts.get(partitionId), numInputs, applicationAttemptId.getAttemptId());
      Roaring64NavigableMap uniqueBlockIdBitMap = Roaring64NavigableMap.bitmapOf();
      blockIdBitmap.forEach(blockId -> {
        long taId = RssTezUtils.getTaskAttemptId(blockId);
        if (taskIdBitmap.contains(taId)) {
          uniqueBlockIdBitMap.add(blockId);
        }
      });
      writeClient.reportUniqueBlocks(new HashSet<>(partitionToServers.get(partitionId)), appId, shuffleId, partitionId,
          uniqueBlockIdBitMap);
    }
  }

  public KeyValuesReader getKeyValuesReader() {
    if (reader == null) {
      return new KeyValuesReader() {
        @Override
        public boolean next() {
          return false;
        }

        @Override
        public Object getCurrentKey() throws IOException {
          throw new IOException("No data available");
        }

        @Override
        public Iterable getCurrentValues() throws IOException {
          throw new IOException("No data available");
        }
      };
    }
    return this.reader.keyValuesReader();
  }

  @VisibleForTesting
  public RMRecordsReader createRMRecordsReader(Set partitionIds) {
    return new RMRecordsReader(appId, shuffleId, partitionIds, partitionToServers, rssConf,
        ConfigUtils.getIntermediateInputKeyClass(conf), ConfigUtils.getIntermediateInputValueClass(conf),
        ConfigUtils.getIntermediateInputKeyComparator(conf), null, false,
        (inc) -> {inputRecordCounter.increment(inc);});
  }

  @Override
  public void reportException(Throwable t) {
    throw new RssException("should never happen!");
  }

  @Override
  public void killSelf(Exception exception, String message) {
    throw new RssException("should never happen!");
  }
}
