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

package org.apache.uniffle.server.storage;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.server.Checker;
import org.apache.uniffle.server.LocalStorageChecker;
import org.apache.uniffle.server.ShuffleDataFlushEvent;
import org.apache.uniffle.server.ShuffleDataReadEvent;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.ShuffleServerMetrics;
import org.apache.uniffle.storage.common.LocalStorage;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.factory.ShuffleHandlerFactory;
import org.apache.uniffle.storage.handler.api.ShuffleDeleteHandler;
import org.apache.uniffle.storage.request.CreateShuffleDeleteHandlerRequest;
import org.apache.uniffle.storage.util.ShuffleStorageUtils;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.uniffle.server.ShuffleServerConf.LOCAL_STORAGE_INITIALIZE_MAX_FAIL_NUMBER;

public class LocalStorageManager extends SingleStorageManager {
  private static final Logger LOG = LoggerFactory.getLogger(LocalStorageManager.class);

  private final List<LocalStorage> localStorages;
  private final List<String> storageBasePaths;
  private final LocalStorageChecker checker;
  private List<LocalStorage> unCorruptedStorages = Lists.newArrayList();
  private final Set<String> corruptedStorages = Sets.newConcurrentHashSet();

  @VisibleForTesting
  LocalStorageManager(ShuffleServerConf conf) {
    super(conf);
    storageBasePaths = conf.get(ShuffleServerConf.RSS_STORAGE_BASE_PATH);
    if (CollectionUtils.isEmpty(storageBasePaths)) {
      throw new IllegalArgumentException("Base path dirs must not be empty");
    }
    long shuffleExpiredTimeoutMs = conf.get(ShuffleServerConf.SHUFFLE_EXPIRED_TIMEOUT_MS);
    long capacity = conf.getSizeAsBytes(ShuffleServerConf.DISK_CAPACITY);
    double highWaterMarkOfWrite = conf.get(ShuffleServerConf.HIGH_WATER_MARK_OF_WRITE);
    double lowWaterMarkOfWrite = conf.get(ShuffleServerConf.LOW_WATER_MARK_OF_WRITE);
    if (highWaterMarkOfWrite < lowWaterMarkOfWrite) {
      throw new IllegalArgumentException("highWaterMarkOfWrite must be larger than lowWaterMarkOfWrite");
    }

    // We must make sure the order of `storageBasePaths` and `localStorages` is same, or some unit test may be fail
    CountDownLatch countDownLatch = new CountDownLatch(storageBasePaths.size());
    AtomicInteger successCount = new AtomicInteger();
    ExecutorService executorService = Executors.newCachedThreadPool();
    LocalStorage[] localStorageArray = new LocalStorage[storageBasePaths.size()];
    for (int i = 0; i < storageBasePaths.size(); i++) {
      final int idx = i;
      String storagePath = storageBasePaths.get(i);
      executorService.submit(() -> {
        try {
          localStorageArray[idx] = LocalStorage.newBuilder()
              .basePath(storagePath)
              .capacity(capacity)
              .lowWaterMarkOfWrite(lowWaterMarkOfWrite)
              .highWaterMarkOfWrite(highWaterMarkOfWrite)
              .shuffleExpiredTimeoutMs(shuffleExpiredTimeoutMs)
              .build();
          successCount.incrementAndGet();
        } catch (Exception e) {
          LOG.error("LocalStorage init failed!", e);
        } finally {
          countDownLatch.countDown();
        }
      });
    }

    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      LOG.error("Failed to wait initializing local storage.", e);
    }
    executorService.shutdown();

    int failedCount = storageBasePaths.size() - successCount.get();
    long maxFailedNumber = conf.getLong(LOCAL_STORAGE_INITIALIZE_MAX_FAIL_NUMBER);
    if (failedCount > maxFailedNumber || successCount.get() == 0) {
      throw new RuntimeException(
          String.format("Initialize %s local storage(s) failed, "
              + "specified local storage paths size: %s, the conf of %s size: %s",
              failedCount, localStorageArray.length, LOCAL_STORAGE_INITIALIZE_MAX_FAIL_NUMBER.key(), maxFailedNumber)
      );
    }
    localStorages = Arrays.stream(localStorageArray).filter(Objects::nonNull).collect(Collectors.toList());
    LOG.info(
        "Succeed to initialize storage paths: {}",
        StringUtils.join(localStorages.stream().map(LocalStorage::getBasePath).collect(Collectors.toList()))
    );
    this.checker = new LocalStorageChecker(conf, localStorages);
  }

  @Override
  public Storage selectStorage(ShuffleDataFlushEvent event) {
    LocalStorage storage = localStorages.get(ShuffleStorageUtils.getStorageIndex(
        localStorages.size(),
        event.getAppId(),
        event.getShuffleId(),
        event.getStartPartition()));
    if (storage.containsWriteHandler(event.getAppId(), event.getShuffleId(), event.getStartPartition())
        && storage.isCorrupted()) {
      throw new RuntimeException("storage " + storage.getBasePath() + " is corrupted");
    }
    if (storage.isCorrupted()) {
      storage = getRepairedStorage(event.getAppId(), event.getShuffleId(), event.getStartPartition());
    }
    return storage;
  }

  @Override
  public Storage selectStorage(ShuffleDataReadEvent event) {

    LocalStorage storage = localStorages.get(ShuffleStorageUtils.getStorageIndex(
        localStorages.size(),
        event.getAppId(),
        event.getShuffleId(),
        event.getStartPartition()));
    if (storage.isCorrupted()) {
      storage = getRepairedStorage(event.getAppId(), event.getShuffleId(), event.getStartPartition());
    }
    return storage;
  }

  @Override
  public void updateWriteMetrics(ShuffleDataFlushEvent event, long writeTime) {
    super.updateWriteMetrics(event, writeTime);
    ShuffleServerMetrics.counterTotalLocalFileWriteDataSize.inc(event.getSize());
  }

  @Override
  public Checker getStorageChecker() {
    return checker;
  }

  @Override
  public void removeResources(String appId, Set<Integer> shuffleSet) {
    for (LocalStorage storage : localStorages) {
      for (Integer shuffleId : shuffleSet) {
        storage.removeHandlers(appId);
        storage.removeResources(RssUtils.generateShuffleKey(appId, shuffleId));
      }
    }
    // delete shuffle data for application
    ShuffleDeleteHandler deleteHandler = ShuffleHandlerFactory.getInstance()
        .createShuffleDeleteHandler(
            new CreateShuffleDeleteHandlerRequest(StorageType.LOCALFILE.name(), new Configuration()));
    deleteHandler.delete(storageBasePaths.toArray(new String[storageBasePaths.size()]), appId);
  }

  @Override
  public void registerRemoteStorage(String appId, RemoteStorageInfo remoteStorageInfo) {
    // ignore
  }

  void repair() {
    boolean hasNewCorruptedStorage = false;
    for (LocalStorage storage : localStorages) {
      if (storage.isCorrupted() && !corruptedStorages.contains(storage.getBasePath())) {
        hasNewCorruptedStorage = true;
        corruptedStorages.add(storage.getBasePath());
      }
    }
    if (hasNewCorruptedStorage) {
      List<LocalStorage> healthyStorages = Lists.newArrayList();
      for (LocalStorage storage : localStorages) {
        if (!storage.isCorrupted()) {
          healthyStorages.add(storage);
        }
      }
      unCorruptedStorages = healthyStorages;
    }
  }

  private synchronized LocalStorage getRepairedStorage(String appId, int shuffleId, int partitionId) {
    repair();
    if (unCorruptedStorages.isEmpty()) {
      throw new RuntimeException("No enough storages");
    }
    return unCorruptedStorages.get(ShuffleStorageUtils.getStorageIndex(
        unCorruptedStorages.size(),
        appId,
        shuffleId,
        partitionId));
  }

  public List<LocalStorage> getStorages() {
    return localStorages;
  }
}
