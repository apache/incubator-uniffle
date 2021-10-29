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

package com.tencent.rss.storage.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Uninterruptibles;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import java.util.Queue;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;

public class DiskItem {

  private static final Logger LOG = LoggerFactory.getLogger(DiskMetaData.class);

  private final long capacity;
  private final String basePath;
  private final double cleanupThreshold;
  private final long cleanIntervalMs;
  private final double highWaterMarkOfWrite;
  private final double lowWaterMarkOfWrite;
  private final long shuffleExpiredTimeoutMs;
  private final Thread cleaner;
  private final Queue<String> expiredShuffleKeys = Queues.newLinkedBlockingQueue();

  private DiskMetaData diskMetaData = new DiskMetaData();
  private boolean canWrite = true;
  private volatile boolean isStopped = false;

  private DiskItem(Builder builder) {
    this.basePath = builder.basePath;
    this.cleanupThreshold = builder.cleanupThreshold;
    this.cleanIntervalMs = builder.cleanIntervalMs;
    this.highWaterMarkOfWrite = builder.highWaterMarkOfWrite;
    this.lowWaterMarkOfWrite = builder.lowWaterMarkOfWrite;
    this.capacity = builder.capacity;
    this.shuffleExpiredTimeoutMs = builder.shuffleExpiredTimeoutMs;

    File baseFolder = new File(basePath);
    try {
      FileUtils.deleteDirectory(baseFolder);
      baseFolder.mkdirs();
    } catch (IOException ioe) {
      LOG.warn("Init base directory " + basePath + " fail, the disk should be corrupted", ioe);
      throw new RuntimeException(ioe);
    }
    long freeSpace = baseFolder.getFreeSpace();
    if (freeSpace < capacity) {
      throw new IllegalArgumentException("Disk Available Capacity " + freeSpace
          + " is smaller than configuration");
    }

    // todo: extract a class named Service, and support stop method. Now
    // we assume that it's enough for one thread per disk. If in testing mode
    // the thread won't be started. cleanInterval should minus the execute time
    // of the method clean.
    cleaner = new Thread(basePath + "-Cleaner") {
      @Override
      public void run() {
        while (!isStopped) {
          try {
            clean();
            // todo: get sleepInterval from configuration
            Uninterruptibles.sleepUninterruptibly(cleanIntervalMs, TimeUnit.MILLISECONDS);
          } catch (Exception e) {
            LOG.error(getName() + " happened exception: ", e);
          }
        }
      }
    };
    cleaner.setDaemon(true);
  }

  public void start() {
    cleaner.start();
  }

  public void stop() {
    isStopped = true;
    Uninterruptibles.joinUninterruptibly(cleaner, 10, TimeUnit.SECONDS);
  }

  public boolean canWrite() {
    if (canWrite) {
      canWrite = diskMetaData.getDiskSize().doubleValue() * 100 / capacity < highWaterMarkOfWrite;
    } else {
      canWrite = diskMetaData.getDiskSize().doubleValue() * 100 / capacity < lowWaterMarkOfWrite;
    }
    return canWrite;
  }

  public String getBasePath() {
    return basePath;
  }

  public void createMetadataIfNotExist(String shuffleKey) {
    diskMetaData.createMetadataIfNotExist(shuffleKey);
  }

  public void updateWrite(String shuffleKey, long delta, List<Integer> partitionList) {
    diskMetaData.updateDiskSize(delta);
    diskMetaData.addShufflePartitionList(shuffleKey, partitionList);
    diskMetaData.updateShuffleSize(shuffleKey, delta);
  }

  public void prepareStartRead(String key) {
    diskMetaData.prepareStartRead(key);
  }

  // todo: refactor DeleteHandler to support shuffleKey level deletion
  @VisibleForTesting
  void clean() {
    for (String key = expiredShuffleKeys.poll(); key != null; key = expiredShuffleKeys.poll()) {
      LOG.info("Remove expired shuffle {}", key);
      removeResources(key);
    }

    if (diskMetaData.getDiskSize().doubleValue() * 100 / capacity < cleanupThreshold) {
      return;
    }

    diskMetaData.getShuffleMetaSet().forEach((shuffleKey) -> {
      // If shuffle data is started to read, shuffle data won't be appended. When shuffle is
      // uploaded totally, the partitions which is not uploaded is empty.
      if (diskMetaData.isShuffleStartRead(shuffleKey)
          && diskMetaData.getNotUploadedPartitions(shuffleKey).isEmpty()
          && isShuffleLongTimeNotRead(shuffleKey)) {
        String shufflePath = ShuffleStorageUtils.getFullShuffleDataFolder(basePath, shuffleKey);
        long start = System.currentTimeMillis();
        try {
          File baseFolder = new File(shufflePath);
          FileUtils.deleteDirectory(baseFolder);
          LOG.info("Clean shuffle {}", shuffleKey);
          removeResources(shuffleKey);
          LOG.info("Delete shuffle data for shuffle [" + shuffleKey + "] with " + shufflePath
              + " cost " + (System.currentTimeMillis() - start) + " ms");
        } catch (Exception e) {
          LOG.warn("Can't delete shuffle data for shuffle [" + shuffleKey + "] with " + shufflePath, e);
        }
      }
    });

  }

  private boolean isShuffleLongTimeNotRead(String shuffleKey) {
    if (diskMetaData.getShuffleLastReadTs(shuffleKey) == -1) {
      return false;
    }
    if (System.currentTimeMillis() - diskMetaData.getShuffleLastReadTs(shuffleKey) > shuffleExpiredTimeoutMs) {
      return true;
    }
    return false;
  }

  public void updateShuffleLastReadTs(String shuffleKey) {
    diskMetaData.updateShuffleLastReadTs(shuffleKey);
  }

  public RoaringBitmap getNotUploadedPartitions(String key) {
    return diskMetaData.getNotUploadedPartitions(key);
  }

  public void updateUploadedShuffle(String shuffleKey, long size, List<Integer> partitions) {
    diskMetaData.updateUploadedShuffleSize(shuffleKey, size);
    diskMetaData.addUploadedShufflePartitionList(shuffleKey, partitions);
  }

  public long getDiskSize() {
    return diskMetaData.getDiskSize().longValue();
  }

  @VisibleForTesting
  DiskMetaData getDiskMetaData() {
    return diskMetaData;
  }

  @VisibleForTesting
  void setDiskMetaData(DiskMetaData diskMetaData) {
    this.diskMetaData = diskMetaData;
  }

  public void addExpiredShuffleKey(String shuffleKey) {
    expiredShuffleKeys.offer(shuffleKey);
  }

  // This is the only place to remove shuffle metadata, clean and gc thread may remove
  // the shuffle metadata concurrently or serially. Force uploader thread may update the
  // shuffle size so gc thread must acquire write lock before updating disk size, and force
  // uploader thread will not get the lock if the shuffle is removed by gc thread, so
  // add the shuffle key back to the expiredShuffleKeys if get lock but fail to acquire write lock.
  void removeResources(String shuffleKey) {
    LOG.info("Start to remove resource of {}", shuffleKey);
    ReadWriteLock lock = diskMetaData.getLock(shuffleKey);
    if (lock == null) {
      LOG.info("Ignore shuffle {} for its resource was removed already", shuffleKey);
      return;
    }

    if (lock.writeLock().tryLock()) {
      try {
        diskMetaData.updateDiskSize(-diskMetaData.getShuffleSize(shuffleKey));
        diskMetaData.remoteShuffle(shuffleKey);
        LOG.info("Finish remove resource of {}, disk size is {} and {} shuffle metadata",
            shuffleKey, diskMetaData.getDiskSize(), diskMetaData.getShuffleMetaSet().size());
      } catch (Exception e) {
        LOG.error("Fail to update disk size", e);
        expiredShuffleKeys.offer(shuffleKey);
      } finally {
        lock.writeLock().unlock();
      }
    } else {
      LOG.info("Fail to get write lock of {}, add it back to expired shuffle queue", shuffleKey);
      expiredShuffleKeys.offer(shuffleKey);
    }
  }

  public ReadWriteLock getLock(String shuffleKey) {
    return diskMetaData.getLock(shuffleKey);
  }

  public long getNotUploadedSize(String key) {
    return diskMetaData.getNotUploadedSize(key);
  }

  public List<String> getSortedShuffleKeys(boolean checkRead, int num) {
    return diskMetaData.getSortedShuffleKeys(checkRead, num);
  }

  public Set<String> getShuffleMetaSet() {
    return diskMetaData.getShuffleMetaSet();
  }

  public void removeShuffle(String shuffleKey, long size, List<Integer> partitions) {
    diskMetaData.removeShufflePartitionList(shuffleKey, partitions);
    diskMetaData.updateDiskSize(-size);
    diskMetaData.updateShuffleSize(shuffleKey, -size);
  }

  public Queue<String> getExpiredShuffleKeys() {
    return expiredShuffleKeys;
  }

  public static class Builder {
    private long capacity;
    private double lowWaterMarkOfWrite;
    private double highWaterMarkOfWrite;
    private double cleanupThreshold;
    private String basePath;
    private long cleanIntervalMs;
    private long shuffleExpiredTimeoutMs;

    private Builder() {
    }

    public Builder capacity(long capacity) {
      this.capacity = capacity;
      return this;
    }

    public Builder lowWaterMarkOfWrite(double lowWaterMarkOfWrite) {
      this.lowWaterMarkOfWrite = lowWaterMarkOfWrite;
      return this;
    }

    public Builder basePath(String basePath) {
      this.basePath = basePath;
      return this;
    }

    public Builder cleanupThreshold(double cleanupThreshold) {
      this.cleanupThreshold = cleanupThreshold;
      return this;
    }

    public Builder highWaterMarkOfWrite(double highWaterMarkOfWrite) {
      this.highWaterMarkOfWrite = highWaterMarkOfWrite;
      return this;
    }

    public Builder cleanIntervalMs(long cleanIntervalMs) {
      this.cleanIntervalMs = cleanIntervalMs;
      return this;
    }

    public Builder shuffleExpiredTimeoutMs(long shuffleExpiredTimeoutMs) {
      this.shuffleExpiredTimeoutMs = shuffleExpiredTimeoutMs;
      return this;
    }

    public DiskItem build() {
      return new DiskItem(this);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }
}
