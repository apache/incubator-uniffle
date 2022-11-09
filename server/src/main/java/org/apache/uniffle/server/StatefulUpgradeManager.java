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

package org.apache.uniffle.server;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import org.apache.uniffle.server.buffer.ShuffleBufferManager;
import org.apache.uniffle.server.state.ShuffleServerState;
import org.apache.uniffle.server.state.StateStore;
import org.apache.uniffle.server.state.StateStoreFactory;

import static org.apache.uniffle.server.ShuffleServerConf.STATEFUL_UPGRADE_STATE_STORE_EXPORT_DATA_LOCATION;
import static org.apache.uniffle.server.ShuffleServerConf.STATEFUL_UPGRADE_STATE_STORE_TYPE;

public class StatefulUpgradeManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(StatefulUpgradeManager.class);

  private final ShuffleServer shuffleServer;
  private final ShuffleServerConf shuffleServerConf;

  private StateStore stateStore;

  public StatefulUpgradeManager(ShuffleServer shuffleServer, ShuffleServerConf shuffleServerConf) {
    this.shuffleServer = shuffleServer;
    this.shuffleServerConf = shuffleServerConf;

    this.stateStore = StateStoreFactory.getInstance().get(
        shuffleServerConf.get(STATEFUL_UPGRADE_STATE_STORE_TYPE),
        shuffleServerConf.get(STATEFUL_UPGRADE_STATE_STORE_EXPORT_DATA_LOCATION)
    );

    // todo: introduce the abstract interface to support more triggers.
    new StatefulUpgradeSignalHandler(this::finalizeAndShutdown).register();
  }

  @VisibleForTesting
  public void finalizeAndMaterializeState() throws Exception {
    ShuffleTaskManager taskManager = shuffleServer.getShuffleTaskManager();
    taskManager.stopValidAppCheck();

    shuffleServer.getServer().stop();
    shuffleServer.markUnhealthy();

    long flushDataStart = System.currentTimeMillis();
    taskManager.persistShuffleData();
    LOGGER.info("Flushing all memory data to persistent storage costs: {} ms",
        System.currentTimeMillis() - flushDataStart);

    long exportStateStart = System.currentTimeMillis();
    stateStore.export(buildInternalState());
    LOGGER.info("Exporting all state to persistent stoarge costs: {} ms",
        System.currentTimeMillis() - exportStateStart);
  }

  public void finalizeAndShutdown() {
    int exitCode = 1;
    try {
      finalizeAndMaterializeState();
    } catch (Exception e) {
      LOGGER.error("Failed to finalize state when doing stateful upgrade.", e);
    }
    System.exit(exitCode);
  }

  private ShuffleServerState buildInternalState() {
    ShuffleTaskManager taskManager = shuffleServer.getShuffleTaskManager();
    ShuffleBufferManager bufferManager = shuffleServer.getShuffleBufferManager();
    ShuffleFlushManager flushManager = shuffleServer.getShuffleFlushManager();

    ShuffleServerState state = ShuffleServerState.builder()
        .partitionsToBlockIds(taskManager.getPartitionsToBlockIds())
        .shuffleTaskInfos(taskManager.getShuffleTaskInfos())
        .requireBufferIds(taskManager.getRequireBufferIds())
        .preAllocatedSize(bufferManager.getPreAllocatedSize())
        .inFlushSize(bufferManager.getInFlushSize())
        .usedMemory(bufferManager.getUsedMemory())
        .readDataMemory(bufferManager.getReadDataMemory())
        .shuffleSizeMap(bufferManager.getShuffleSizeMap())
        .committedBlockIds(flushManager.getCommittedBlockIds())
        .build();

    return state;
  }

  public boolean recoverState() throws Exception {
    long restoreStateStart = System.currentTimeMillis();
    ShuffleServerState state = stateStore.restore();
    LOGGER.info("Restore the state from external persistent storage costs: {} ms",
        System.currentTimeMillis() - restoreStateStart);

    ShuffleTaskManager taskManager = shuffleServer.getShuffleTaskManager();
    taskManager.setPartitionsToBlockIds(state.getPartitionsToBlockIds());
    // refresh the app heartbeat time to avoid cleaning up
    state.getShuffleTaskInfos().values().stream().forEach(x -> x.setCurrentTimes(System.currentTimeMillis()));
    taskManager.setShuffleTaskInfos(state.getShuffleTaskInfos());
    taskManager.setRequireBufferIds(state.getRequireBufferIds());

    ShuffleBufferManager bufferManager = shuffleServer.getShuffleBufferManager();
    bufferManager.setReadDataMemory(new AtomicLong(state.getReadDataMemory()));
    bufferManager.setInFlushSize(new AtomicLong(state.getInFlushSize()));
    bufferManager.setUsedMemory(new AtomicLong(state.getUsedMemory()));
    bufferManager.setPreAllocatedSize(new AtomicLong(state.getPreAllocatedSize()));
    bufferManager.setShuffleSizeMap(state.getShuffleSizeMap());

    ShuffleFlushManager flushManager = shuffleServer.getShuffleFlushManager();
    flushManager.setCommittedBlockIds(state.getCommittedBlockIds());

    return true;
  }

  class StatefulUpgradeSignalHandler implements SignalHandler {
    private static final String SIGNAL_NAME = "TERM";
    private final Runnable handler;

    public StatefulUpgradeSignalHandler(Runnable handler) {
      this.handler = handler;
    }

    public void register() {
      Signal signal = new Signal(SIGNAL_NAME);
      Signal.handle(signal, this);
    }

    @Override
    public void handle(Signal signal) {
      LOGGER.info("Capture the signal of {}", signal.getName());
      handler.run();
      LOGGER.info("Succeed to handle the signal.");
    }
  }
}
