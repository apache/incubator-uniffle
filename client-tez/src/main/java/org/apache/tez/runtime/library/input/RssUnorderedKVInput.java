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

package org.apache.tez.runtime.library.input;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.tez.common.RssTezUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.ProgressFailedException;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.readers.UnorderedKVReader;
import org.apache.tez.runtime.library.common.shuffle.ShuffleEventHandler;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.shuffle.impl.RssShuffleManager;
import org.apache.tez.runtime.library.common.shuffle.impl.RssSimpleFetchedInputAllocator;
import org.apache.tez.runtime.library.common.shuffle.impl.ShuffleInputEventHandlerImpl;
import org.apache.tez.runtime.library.common.shuffle.impl.SimpleFetchedInputAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.exception.RssException;

import static org.apache.tez.common.RssTezConfig.RSS_SHUFFLE_DESTINATION_VERTEX_ID;
import static org.apache.tez.common.RssTezConfig.RSS_SHUFFLE_SOURCE_VERTEX_ID;

/**
 * {@link RssUnorderedKVInput} provides unordered key value input by bringing together (shuffling) a
 * set of distributed data and providing a unified view to that data. There are no ordering
 * constraints applied by this input.
 */
@Public
public class RssUnorderedKVInput extends AbstractLogicalInput {
  private static final Logger LOG = LoggerFactory.getLogger(RssUnorderedKVInput.class);

  private Configuration conf;
  private RssShuffleManager rssShuffleManager;
  private final BlockingQueue<Event> pendingEvents = new LinkedBlockingQueue<>();
  private long firstEventReceivedTime = -1;
  private MemoryUpdateCallbackHandler memoryUpdateCallbackHandler;

  @SuppressWarnings("rawtypes")
  private UnorderedKVReader kvReader;

  private final AtomicBoolean isStarted = new AtomicBoolean(false);
  private TezCounter inputRecordCounter;

  private SimpleFetchedInputAllocator inputManager;
  private ShuffleEventHandler inputEventHandler;
  private int shuffleId;
  private ApplicationAttemptId applicationAttemptId;

  public RssUnorderedKVInput(InputContext inputContext, int numPhysicalInputs) {
    super(inputContext, numPhysicalInputs);
  }

  @Override
  public synchronized List<Event> initialize() throws Exception {
    LOG.info("RssUnorderedKVInput initialize， num of physicalInputs:{}", getNumPhysicalInputs());
    Preconditions.checkArgument(getNumPhysicalInputs() != -1, "Number of Inputs has not been set");
    this.conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());

    if (getNumPhysicalInputs() == 0) {
      getContext().requestInitialMemory(0L, null);
      isStarted.set(true);
      getContext().inputIsReady();
      LOG.info(
          "input fetch not required since there are 0 physical inputs for input vertex: "
              + getContext().getSourceVertexName());
      return Collections.emptyList();
    } else {
      long initialMemReq = getInitialMemoryReq();
      memoryUpdateCallbackHandler = new MemoryUpdateCallbackHandler();
      this.getContext().requestInitialMemory(initialMemReq, memoryUpdateCallbackHandler);
    }

    this.conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, getContext().getWorkDirs());
    this.inputRecordCounter =
        getContext().getCounters().findCounter(TaskCounter.INPUT_RECORDS_PROCESSED);

    TezTaskAttemptID taskAttemptId =
        TezTaskAttemptID.fromString(
            RssTezUtils.uniqueIdentifierToAttemptId(getContext().getUniqueIdentifier()));
    TezVertexID tezVertexID = taskAttemptId.getTaskID().getVertexID();
    TezDAGID tezDAGID = tezVertexID.getDAGId();
    int sourceVertexId = this.conf.getInt(RSS_SHUFFLE_SOURCE_VERTEX_ID, -1);
    int destinationVertexId = this.conf.getInt(RSS_SHUFFLE_DESTINATION_VERTEX_ID, -1);
    if (sourceVertexId == -1) {
      throw new RssException("sourceVertexId should not be -1");
    }
    if (destinationVertexId == -1) {
      throw new RssException("destinationVertexId should not be -1");
    }
    this.shuffleId =
        RssTezUtils.computeShuffleId(tezDAGID.getId(), sourceVertexId, destinationVertexId);
    this.applicationAttemptId =
        ApplicationAttemptId.newInstance(
            getContext().getApplicationId(), getContext().getDAGAttemptNumber());
    return Collections.emptyList();
  }

  @Override
  public synchronized void start() throws IOException {
    if (!isStarted.get()) {
      ////// Initial configuration
      memoryUpdateCallbackHandler.validateUpdateReceived();
      CompressionCodec codec;
      if (ConfigUtils.isIntermediateInputCompressed(conf)) {
        Class<? extends CompressionCodec> codecClass =
            ConfigUtils.getIntermediateInputCompressorClass(conf, DefaultCodec.class);
        codec = ReflectionUtils.newInstance(codecClass, conf);
      } else {
        codec = null;
      }

      boolean ifileReadAhead =
          conf.getBoolean(
              TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD,
              TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT);
      int ifileReadAheadLength = 0;
      int ifileBufferSize = 0;

      if (ifileReadAhead) {
        ifileReadAheadLength =
            conf.getInt(
                TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES,
                TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT);
      }
      ifileBufferSize =
          conf.getInt(
              "io.file.buffer.size", TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_BUFFER_SIZE_DEFAULT);

      LOG.info(
          "RssUnorderedKVInput, totalMemoryAvailable:{}, available memory:{}",
          getContext().getTotalMemoryAvailableToTask(),
          memoryUpdateCallbackHandler.getMemoryAssigned());

      boolean compositeFetch = ShuffleUtils.isTezShuffleHandler(conf);

      this.inputManager =
          new RssSimpleFetchedInputAllocator(
              TezUtilsInternal.cleanVertexName(getContext().getSourceVertexName()),
              getContext().getUniqueIdentifier(),
              getContext().getDagIdentifier(),
              conf,
              getContext().getTotalMemoryAvailableToTask(),
              memoryUpdateCallbackHandler.getMemoryAssigned(),
              applicationAttemptId.toString());

      this.rssShuffleManager =
          new RssShuffleManager(
              getContext(),
              conf,
              getNumPhysicalInputs(),
              ifileBufferSize,
              ifileReadAhead,
              ifileReadAheadLength,
              codec,
              inputManager,
              shuffleId,
              applicationAttemptId);

      this.inputEventHandler =
          new ShuffleInputEventHandlerImpl(
              getContext(),
              rssShuffleManager,
              inputManager,
              codec,
              ifileReadAhead,
              ifileReadAheadLength,
              compositeFetch);

      ////// End of Initial configuration

      this.rssShuffleManager.run();
      this.kvReader =
          createReader(
              inputRecordCounter, codec, ifileBufferSize, ifileReadAhead, ifileReadAheadLength);
      List<Event> pending = new LinkedList<>();
      pendingEvents.drainTo(pending);
      if (pending.size() > 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              getContext().getSourceVertexName()
                  + ": "
                  + "NoAutoStart delay in processing first event: "
                  + (System.currentTimeMillis() - firstEventReceivedTime));
        }
        inputEventHandler.handleEvents(pending);
      }
      isStarted.set(true);
    }
  }

  @Override
  public synchronized KeyValueReader getReader() throws Exception {
    Preconditions.checkState(isStarted.get(), "Must start input before invoking this method");
    if (getNumPhysicalInputs() == 0) {
      return new KeyValueReader() {
        @Override
        public boolean next() throws IOException {
          getContext().notifyProgress();
          hasCompletedProcessing();
          completedProcessing = true;
          return false;
        }

        @Override
        public Object getCurrentKey() throws IOException {
          throw new RssException("No data available in Input");
        }

        @Override
        public Object getCurrentValue() throws IOException {
          throw new RssException("No data available in Input");
        }
      };
    }
    return this.kvReader;
  }

  @Override
  public void handleEvents(List<Event> inputEvents) throws IOException {
    ShuffleEventHandler inputEventHandlerLocalRef;
    synchronized (this) {
      if (getNumPhysicalInputs() == 0) {
        throw new RssException("No input events expected as numInputs is 0");
      }
      if (!isStarted.get()) {
        if (firstEventReceivedTime == -1) {
          firstEventReceivedTime = System.currentTimeMillis();
        }
        // This queue will keep growing if the Processor decides never to
        // start the event. The Input, however has no idea, on whether start
        // will be invoked or not.
        pendingEvents.addAll(inputEvents);
        return;
      }
      inputEventHandlerLocalRef = inputEventHandler;
    }
    inputEventHandlerLocalRef.handleEvents(inputEvents);
  }

  @Override
  public synchronized List<Event> close() throws Exception {
    if (this.inputEventHandler != null) {
      this.inputEventHandler.logProgress(true);
    }

    if (this.rssShuffleManager != null) {
      this.rssShuffleManager.shutdown();
    }

    long dataSize =
        getContext().getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_DECOMPRESSED).getValue();
    getContext().getStatisticsReporter().reportDataSize(dataSize);
    long inputRecords =
        getContext().getCounters().findCounter(TaskCounter.INPUT_RECORDS_PROCESSED).getValue();
    getContext().getStatisticsReporter().reportItemsProcessed(inputRecords);
    return null;
  }

  private long getInitialMemoryReq() {
    return SimpleFetchedInputAllocator.getInitialMemoryReq(
        conf, getContext().getTotalMemoryAvailableToTask());
  }

  @SuppressWarnings("rawtypes")
  private UnorderedKVReader createReader(
      TezCounter inputRecordCounter,
      CompressionCodec codec,
      int ifileBufferSize,
      boolean ifileReadAheadEnabled,
      int ifileReadAheadLength)
      throws IOException {
    return new UnorderedKVReader(
        rssShuffleManager,
        conf,
        null,
        ifileReadAheadEnabled,
        0,
        ifileBufferSize,
        inputRecordCounter,
        getContext());
  }

  private static final Set<String> CONF_KEYS = new HashSet<>();

  static {
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_IO_FILE_BUFFER_SIZE);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_USE_ASYNC_HTTP);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_FAILURES_LIMIT);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_NOTIFY_READERROR);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_CONNECT_TIMEOUT);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_ENABLED);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_MAX_CONNECTIONS);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_BUFFER_SIZE);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_SSL);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_VERIFY_DISK_CHECKSUM);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCHER_USE_SHARED_POOL);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_SHARED_FETCH);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_CONVERT_USER_PAYLOAD_TO_HISTORY_TEXT);
    CONF_KEYS.add(TezConfiguration.TEZ_COUNTERS_MAX);
    CONF_KEYS.add(TezConfiguration.TEZ_COUNTERS_GROUP_NAME_MAX_LENGTH);
    CONF_KEYS.add(TezConfiguration.TEZ_COUNTERS_COUNTER_NAME_MAX_LENGTH);
    CONF_KEYS.add(TezConfiguration.TEZ_COUNTERS_MAX_GROUPS);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_CLEANUP_FILES_ON_INTERRUPT);
    CONF_KEYS.add(Constants.TEZ_RUNTIME_TASK_MEMORY);
    CONF_KEYS.add(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID);
  }

  // Maybe add helper methods to extract keys
  // Maybe add constants or an Enum to access the keys

  @InterfaceAudience.Private
  public static Set<String> getConfigurationKeySet() {
    return Collections.unmodifiableSet(CONF_KEYS);
  }

  @Override
  public float getProgress() throws ProgressFailedException, InterruptedException {
    try {
      return kvReader.getProgress();
    } catch (IOException e) {
      throw new ProgressFailedException("getProgress encountered IOException ", e);
    }
  }
}
