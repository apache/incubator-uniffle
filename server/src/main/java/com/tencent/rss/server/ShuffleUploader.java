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

package com.tencent.rss.server;

import java.io.File;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.common.util.ByteUnit;
import com.tencent.rss.storage.common.DiskItem;
import com.tencent.rss.storage.common.ShuffleFileInfo;
import com.tencent.rss.storage.factory.ShuffleUploadHandlerFactory;
import com.tencent.rss.storage.handler.api.ShuffleUploadHandler;
import com.tencent.rss.storage.request.CreateShuffleUploadHandlerRequest;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import com.tencent.rss.storage.util.ShuffleUploadResult;
import com.tencent.rss.storage.util.StorageType;

/**
 * ShuffleUploader contains force mode and normal mode, which is decided by the remain
 * space of the disk, and it can be retrieved from the metadata. In force mode, shuffle
 * data will be upload to remote storage whether the shuffle is finished or not.
 * In normal mode, shuffle data will be upload only when it is finished and the shuffle
 * files Both mode will leave the uploaded data to be delete by the cleaner.
 *
 * The underlying handle to upload files to remote storage support HDFS at present, but
 * it will support other storage type (eg, COS, OZONE) and will add more optional parameters,
 * so ShuffleUploader use Joshua Bloch builder pattern to construct and validate the parameters.
 *
 */
public class ShuffleUploader {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleUploader.class);

  private final DiskItem diskItem;
  private final int uploadThreadNum;
  private final long uploadIntervalMS;
  private final long uploadCombineThresholdMB;
  private final long referenceUploadSpeedMBS;
  private final StorageType remoteStorageType;
  private final String hdfsBathPath;
  private final String serverId;
  private final Configuration hadoopConf;
  private final Thread daemonThread;
  private final long maxShuffleSize;
  private final long maxForceUploadExpireTimeS;

  private final ExecutorService executorService;
  private volatile boolean isStopped;

  public ShuffleUploader(Builder builder) {
    this.diskItem = builder.diskItem;
    this.uploadThreadNum = builder.uploadThreadNum;
    this.uploadIntervalMS = builder.uploadIntervalMS;
    this.uploadCombineThresholdMB = builder.uploadCombineThresholdMB;
    this.referenceUploadSpeedMBS = builder.referenceUploadSpeedMBS;
    this.remoteStorageType = builder.remoteStorageType;
    // HDFS related parameters
    this.hdfsBathPath = builder.hdfsBathPath;
    this.serverId = builder.serverId;
    this.hadoopConf = builder.hadoopConf;
    this.maxShuffleSize = builder.maxShuffleSize;
    this.maxForceUploadExpireTimeS = builder.maxForceUploadExpireTimeS;

    Runnable runnable = () -> {
      run();
    };

    daemonThread = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(diskItem.getBasePath() + " - ShuffleUploader-%d")
        .build()
        .newThread(runnable);

    executorService = Executors.newFixedThreadPool(
        uploadThreadNum,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat(diskItem.getBasePath() + " ShuffleUploadWorker-%d")
            .build());
  }

  public static class Builder {
    private DiskItem diskItem;
    private int uploadThreadNum;
    private long uploadIntervalMS;
    private long uploadCombineThresholdMB;
    private long referenceUploadSpeedMBS;
    private StorageType remoteStorageType;
    private String hdfsBathPath;
    private String serverId;
    private Configuration hadoopConf;
    private long maxShuffleSize = (long) ByteUnit.MiB.toBytes(256);
    private long maxForceUploadExpireTimeS;

    public Builder() {
      // use HDFS and not force upload by default
      this.remoteStorageType = StorageType.HDFS;
    }

    public Builder diskItem(DiskItem diskItem) {
      this.diskItem = diskItem;
      return this;
    }

    public Builder uploadThreadNum(int uploadThreadNum) {
      this.uploadThreadNum = uploadThreadNum;
      return this;
    }

    public Builder uploadIntervalMS(long uploadIntervalMS) {
      this.uploadIntervalMS = uploadIntervalMS;
      return this;
    }

    public Builder uploadCombineThresholdMB(long uploadCombineThresholdMB) {
      this.uploadCombineThresholdMB = uploadCombineThresholdMB;
      return this;
    }

    public Builder referenceUploadSpeedMBS(long referenceUploadSpeedMBS) {
      this.referenceUploadSpeedMBS = referenceUploadSpeedMBS;
      return this;
    }

    public Builder remoteStorageType(StorageType remoteStorageType) {
      this.remoteStorageType = remoteStorageType;
      return this;
    }

    public Builder hdfsBathPath(String hdfsBathPath) {
      this.hdfsBathPath = hdfsBathPath;
      return this;
    }

    public Builder serverId(String serverId) {
      this.serverId = serverId;
      return this;
    }

    public Builder hadoopConf(Configuration hadoopConf) {
      this.hadoopConf = hadoopConf;
      return this;
    }

    public Builder maxShuffleSize(long maxShuffleSize) {
      this.maxShuffleSize = maxShuffleSize;
      return this;
    }

    public Builder maxForceUploadExpireTimeS(long maxForceUploadExpireTimeS) {
      this.maxForceUploadExpireTimeS = maxForceUploadExpireTimeS;
      return this;
    }

    public ShuffleUploader build() throws IllegalArgumentException {
      validate();
      return new ShuffleUploader(this);
    }

    private void validate() throws IllegalArgumentException {
      // check common parameters
      if (diskItem == null) {
        throw new IllegalArgumentException("Disk item is not set");
      }

      if (uploadThreadNum <= 0) {
        throw new IllegalArgumentException("Upload thread num must > 0");
      }

      if (uploadIntervalMS <= 0) {
        throw new IllegalArgumentException("Upload interval must > 0");
      }

      if (uploadCombineThresholdMB <= 0) {
        throw new IllegalArgumentException("Upload combine threshold num must > 0");
      }

      if (referenceUploadSpeedMBS <= 0) {
        throw new IllegalArgumentException("Upload reference speed must > 0");
      }

      if (maxShuffleSize <= 0) {
        throw new IllegalArgumentException("maxShuffleSize must be positive");
      }

      // check remote storage related parameters
      if (remoteStorageType == StorageType.HDFS) {
        if (StringUtils.isEmpty(hdfsBathPath)) {
          throw new IllegalArgumentException("HDFS base path is not set");
        }

        if (StringUtils.isEmpty(serverId)) {
          throw new IllegalArgumentException("Server id of file prefix is not set");
        }

        if (hadoopConf == null) {
          throw new IllegalArgumentException("HDFS configuration is not set");
        }

      } else {
        throw new IllegalArgumentException(remoteStorageType + " remote storage type is not supported!");
      }
    }
  }

  public void run() {
    while (!isStopped) {
      try {
        long start = System.currentTimeMillis();
        upload();
        long uploadTime = System.currentTimeMillis() - start;
        ShuffleServerMetrics.counterTotalUploadTimeS.inc(uploadTime / 1000.0);

        if (uploadTime < uploadIntervalMS) {
          Uninterruptibles.sleepUninterruptibly(uploadIntervalMS - uploadTime, TimeUnit.MILLISECONDS);
        }
      } catch (Exception e) {
        LOG.error("{} - upload exception: {}", Thread.currentThread().getName(), ExceptionUtils.getStackTrace(e));
      }
    }
  }

  public void start() {
    daemonThread.start();
  }

  public void stop() {
    isStopped = true;
    executorService.shutdownNow();
    Uninterruptibles.joinUninterruptibly(daemonThread, 5, TimeUnit.SECONDS);
  }

  // upload is a blocked until uploading success or timeout exception
  @VisibleForTesting
  void upload() {

    boolean forceUpload = !diskItem.canWrite();
    LOG.debug("Upload force mode is {}, disk size is {}, shuffle keys are {}",
        forceUpload, diskItem.getDiskSize(), diskItem.getShuffleMetaSet());

    List<ShuffleFileInfo> shuffleFileInfos = selectShuffleFiles(uploadThreadNum, forceUpload);
    if (shuffleFileInfos == null || shuffleFileInfos.isEmpty()) {
      return;
    }

    List<Callable<ShuffleUploadResult>> callableList = Lists.newLinkedList();
    List<ReadWriteLock> locks = Lists.newArrayList();
    long totalSize = 0;
    long maxSize = 0;
    for (ShuffleFileInfo shuffleFileInfo : shuffleFileInfos) {
      if (!shuffleFileInfo.isValid()) {
        continue;
      }
      ReadWriteLock lock = diskItem.getLock(shuffleFileInfo.getKey());
      if (lock == null) {
        continue;
      }

      boolean locked = false;
      if (forceUpload) {
        locked = lock.writeLock().tryLock();
        if (!locked) {
          continue;
        }
        locks.add(lock);
      }
      totalSize = totalSize + shuffleFileInfo.getSize();
      maxSize = Math.max(maxSize, shuffleFileInfo.getSize());
      Callable<ShuffleUploadResult> callable = () -> {
        try {
          CreateShuffleUploadHandlerRequest request =
              new CreateShuffleUploadHandlerRequest.Builder()
                  .remoteStorageType(remoteStorageType)
                  .remoteStorageBasePath(
                      ShuffleStorageUtils.getFullShuffleDataFolder(hdfsBathPath, shuffleFileInfo.getKey()))
                  .hadoopConf(hadoopConf)
                  .hdfsFilePrefix(serverId)
                  .combineUpload(shuffleFileInfo.shouldCombine(uploadCombineThresholdMB))
                  .build();

          ShuffleUploadHandler handler = getHandlerFactory().createShuffleUploadHandler(request);
          ShuffleUploadResult shuffleUploadResult = handler.upload(
              shuffleFileInfo.getDataFiles(),
              shuffleFileInfo.getIndexFiles(),
              shuffleFileInfo.getPartitions());
          if (shuffleUploadResult == null) {
            return null;
          }
          ShuffleServerMetrics.counterTotalUploadSize.inc(shuffleUploadResult.getSize());
          shuffleUploadResult.setShuffleKey(shuffleFileInfo.getKey());
          return shuffleUploadResult;
        } catch (Exception e) {
          LOG.error("Fail to construct upload callable list {}", ExceptionUtils.getStackTrace(e));
          return null;
        }
      };
      callableList.add(callable);
    }

    long uploadTimeoutS = calculateUploadTime(maxSize, totalSize, forceUpload);
    LOG.info("Start to upload {} shuffle info maxSize {} totalSize {} and timeout is {} Seconds and disk size is {}",
        callableList.size(), maxSize, totalSize, uploadTimeoutS, diskItem.getDiskSize());
    long startTimeMs = System.currentTimeMillis();
    try {
      List<Future<ShuffleUploadResult>> futures =
          executorService.invokeAll(callableList, uploadTimeoutS, TimeUnit.SECONDS);
      for (Future<ShuffleUploadResult> future : futures) {
        if (future.isDone()) {
          ShuffleUploadResult shuffleUploadResult = future.get();
          if (shuffleUploadResult == null) {
            LOG.info("shuffleUploadResult is empty..");
            continue;
          }
          LOG.info("force mode enable {} upload shuffle {} partitions: {}", forceUpload,
              shuffleUploadResult.getShuffleKey(), shuffleUploadResult.getPartitions().size());
          LOG.debug("upload partitions detail: {}", shuffleUploadResult.getPartitions());
          if (forceUpload) {
            deleteForceUploadPartitions(shuffleUploadResult.getShuffleKey(), shuffleUploadResult.getPartitions());
            diskItem.removeShuffle(shuffleUploadResult.getShuffleKey(), shuffleUploadResult.getSize(),
                shuffleUploadResult.getPartitions());
          } else {
            String shuffleKey = shuffleUploadResult.getShuffleKey();
            diskItem.updateUploadedShuffle(shuffleKey, shuffleUploadResult.getSize(),
                shuffleUploadResult.getPartitions());
          }
        } else {
          future.cancel(true);
        }
      }
    } catch (Exception e) {
      LOG.error(
          "Fail to upload {}, {}",
          shuffleFileInfos.stream().map(ShuffleFileInfo::getKey).collect(Collectors.joining("\n")),
          ExceptionUtils.getStackTrace(e));
    } finally {
      for (ReadWriteLock lock : locks) {
        lock.writeLock().unlock();
      }
      LOG.info("{} upload use {}ms and disk size is {}",
          Thread.currentThread().getName(), System.currentTimeMillis() - startTimeMs, diskItem.getDiskSize());
    }
  }

  private void deleteForceUploadPartitions(String shuffleKey, List<Integer> partitions) {
    int failDeleteFiles = 0;
    for (int partition : partitions) {
      String filePrefix = ShuffleStorageUtils.generateAbsoluteFilePrefix(
          diskItem.getBasePath(), shuffleKey, partition, serverId);
      String dataFileName = ShuffleStorageUtils.generateDataFileName(filePrefix);
      String indexFileName = ShuffleStorageUtils.generateIndexFileName(filePrefix);
      File dataFile = new File(dataFileName);
      boolean suc = dataFile.delete();
      if (!suc) {
        failDeleteFiles++;
      }
      File indexFile = new File(indexFileName);
      suc = indexFile.delete();
      if (!suc) {
        failDeleteFiles++;
      }
      if (failDeleteFiles > 0) {
        LOG.error("Force upload process delete file fail {} times", failDeleteFiles);
      }
    }
  }

  @VisibleForTesting
  long calculateUploadTime(long maxSize, long totalSize, boolean forceUpload) {
    long defaultUploadTimeoutS = 1L;
    long size = 0;
    if (maxSize > totalSize / uploadThreadNum) {
      size = maxSize;
    } else {
      size = totalSize / uploadThreadNum;
    }
    long cur = ByteUnit.BYTE.toMiB(size) / referenceUploadSpeedMBS;
    if (cur <= defaultUploadTimeoutS) {
      cur =  defaultUploadTimeoutS * 2;
    } else {
      cur = cur * 2;
    }

    // Use a hard upload limitation in force mode
    if (forceUpload) {
      cur = Math.min(cur, maxForceUploadExpireTimeS);
    }

    return cur;
  }

  @VisibleForTesting
  List<ShuffleFileInfo> selectShuffleFiles(int num, boolean forceUpload) {
    List<ShuffleFileInfo> shuffleFileInfoList = Lists.newLinkedList();
    List<String> shuffleKeys = diskItem.getSortedShuffleKeys(!forceUpload, num);
    if (shuffleKeys.isEmpty()) {
      return Lists.newArrayList();
    }

    LOG.info("Get {} candidate shuffles {}", shuffleKeys.size(), shuffleKeys);
    for (String shuffleKey : shuffleKeys) {
      List<Integer> partitions = getNotUploadedPartitions(shuffleKey);
      long sz = getNotUploadedSize(shuffleKey);
      if (partitions.isEmpty() || sz <= 0) {
        LOG.warn("upload shuffle data empty shuffle {} size {} partitions {}", shuffleKey, sz, partitions);
        continue;
      }
      ShuffleFileInfo shuffleFileInfo = new ShuffleFileInfo();
      shuffleFileInfo.setKey(shuffleKey);
      for (int partition : partitions) {
        long size = addPartition(shuffleFileInfo, partition);
        shuffleFileInfo.setSize(shuffleFileInfo.getSize() + size);
        if (shuffleFileInfo.getSize() > maxShuffleSize) {
          shuffleFileInfoList.add(shuffleFileInfo);

          // Restrict the max upload segment to uploadThreadNum to make the
          // uploading finish and release the disk space asap in force mode.
          if (forceUpload && shuffleFileInfoList.size() >= uploadThreadNum) {
            return shuffleFileInfoList;
          }

          shuffleFileInfo = new ShuffleFileInfo();
          shuffleFileInfo.setKey(shuffleKey);
        }
      }

      if (!shuffleFileInfo.isEmpty()) {
        shuffleFileInfoList.add(shuffleFileInfo);
      }

      if (forceUpload && shuffleFileInfoList.size() >= uploadThreadNum) {
        return shuffleFileInfoList;
      }
    }
    return shuffleFileInfoList;
  }

  private List getNotUploadedPartitions(String key) {
    RoaringBitmap bitmap = diskItem.getNotUploadedPartitions(key);
    List<Integer> partitionList = Lists.newArrayList();
    for (int p : bitmap) {
      partitionList.add(p);
    }
    partitionList.sort(Integer::compare);
    return partitionList;
  }

  private long getNotUploadedSize(String key) {
    return diskItem.getNotUploadedSize(key);
  }

  @VisibleForTesting
  ShuffleUploadHandlerFactory getHandlerFactory() {
    return ShuffleUploadHandlerFactory.getInstance();
  }


  private long addPartition(ShuffleFileInfo shuffleFileInfo, int partition) {
      String filePrefix = ShuffleStorageUtils.generateAbsoluteFilePrefix(
          diskItem.getBasePath(), shuffleFileInfo.getKey(), partition, serverId);
      String dataFileName = ShuffleStorageUtils.generateDataFileName(filePrefix);
      String indexFileName = ShuffleStorageUtils.generateIndexFileName(filePrefix);

      File dataFile = new File(dataFileName);
      if (!dataFile.exists()) {
        LOG.error("{} don't exist!", dataFileName);
        return 0;
      }

      File indexFile = new File(indexFileName);
      if (!indexFile.exists()) {
        LOG.error("{} don't exist!", indexFileName);
        return 0;
      }

      shuffleFileInfo.getDataFiles().add(dataFile);
      shuffleFileInfo.getIndexFiles().add(indexFile);
      shuffleFileInfo.getPartitions().add(partition);
      return dataFile.length();
  }
}
