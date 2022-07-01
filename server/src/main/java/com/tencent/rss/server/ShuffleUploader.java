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

package com.tencent.rss.server;

import java.io.File;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.common.util.ByteUnit;
import com.tencent.rss.storage.common.LocalStorage;
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

  private final LocalStorage localStorage;
  private final int uploadThreadNum;
  private final long uploadIntervalMS;
  private final long uploadCombineThresholdMB;
  private final long referenceUploadSpeedMBS;
  private final StorageType remoteStorageType;
  private final String hdfsBasePath;
  private final String serverId;
  private final Configuration hadoopConf;
  private final Thread daemonThread;
  private final long maxShuffleSize;
  private final long maxForceUploadExpireTimeS;
  private final double cleanupThreshold;

  private final ExecutorService executorService;
  private volatile boolean isStopped;

  public ShuffleUploader(Builder builder) {
    this.localStorage = builder.localStorage;
    this.uploadThreadNum = builder.uploadThreadNum;
    this.uploadIntervalMS = builder.uploadIntervalMS;
    this.uploadCombineThresholdMB = builder.uploadCombineThresholdMB;
    this.referenceUploadSpeedMBS = builder.referenceUploadSpeedMBS;
    this.remoteStorageType = builder.remoteStorageType;
    // HDFS related parameters
    this.hdfsBasePath = builder.hdfsBasePath;
    this.serverId = builder.serverId;
    this.hadoopConf = builder.hadoopConf;
    this.maxShuffleSize = builder.maxShuffleSize;
    this.maxForceUploadExpireTimeS = builder.maxForceUploadExpireTimeS;
    this.cleanupThreshold = builder.cleanupThreshold;

    Runnable runnable = () -> {
      run();
    };

    daemonThread = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(localStorage.getBasePath() + " - ShuffleUploader-%d")
        .build()
        .newThread(runnable);

    executorService = Executors.newFixedThreadPool(
        uploadThreadNum,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat(localStorage.getBasePath() + " ShuffleUploadWorker-%d")
            .build());
  }

  public static class Builder {
    private LocalStorage localStorage;
    private int uploadThreadNum;
    private long uploadIntervalMS;
    private long uploadCombineThresholdMB;
    private long referenceUploadSpeedMBS;
    private StorageType remoteStorageType;
    private String hdfsBasePath;
    private String serverId;
    private Configuration hadoopConf;
    private long maxShuffleSize = (long) ByteUnit.MiB.toBytes(256);
    private long maxForceUploadExpireTimeS;
    private double cleanupThreshold;

    public Builder() {
      // use HDFS and not force upload by default
      this.remoteStorageType = StorageType.HDFS;
    }

    public Builder localStorage(LocalStorage localStorage) {
      this.localStorage = localStorage;
      return this;
    }

    public Builder configuration(ShuffleServerConf conf) {
      uploadThreadNum = conf.get(ShuffleServerConf.UPLOADER_THREAD_NUM);
      uploadIntervalMS = conf.get(ShuffleServerConf.UPLOADER_INTERVAL_MS);
      uploadCombineThresholdMB = conf.get(ShuffleServerConf.UPLOAD_COMBINE_THRESHOLD_MB);
      referenceUploadSpeedMBS = conf.get(ShuffleServerConf.REFERENCE_UPLOAD_SPEED_MBS);
      cleanupThreshold = conf.get(ShuffleServerConf.CLEANUP_THRESHOLD);

      hdfsBasePath = conf.get(ShuffleServerConf.UPLOADER_BASE_PATH);
      if (StringUtils.isEmpty(hdfsBasePath)) {
        throw new IllegalArgumentException("hdfsBasePath couldn't be empty");
      }

      remoteStorageType = StorageType.valueOf(conf.getString(ShuffleServerConf.UPLOAD_STORAGE_TYPE));
      if (StorageType.LOCALFILE.equals(remoteStorageType)) {
        throw new IllegalArgumentException("uploadRemoteStorageType couldn't be LOCALFILE or FILE");
      }
      maxShuffleSize = conf.get(ShuffleServerConf.SHUFFLE_MAX_UPLOAD_SIZE);
      double maxShuffleForceUploadTimeRatio = conf.get(ShuffleServerConf.SHUFFLE_MAX_FORCE_UPLOAD_TIME_RATIO);
      long pendingEventTimeoutS = conf.get(ShuffleServerConf.PENDING_EVENT_TIMEOUT_SEC);
      this.maxForceUploadExpireTimeS = (long) (pendingEventTimeoutS * (maxShuffleForceUploadTimeRatio / 100.0));
      this.hadoopConf = conf.getHadoopConf();
      return this;
    }

    public Builder serverId(String serverId) {
      this.serverId = serverId;
      return this;
    }

    @VisibleForTesting
    Builder maxForceUploadExpireTimeS(long time) {
      this.maxForceUploadExpireTimeS = time;
      return this;
    }

    public ShuffleUploader build() throws IllegalArgumentException {
      return new ShuffleUploader(this);
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

    boolean forceUpload = !localStorage.canWrite();
    LOG.debug("Upload force mode is {}, disk size is {}, shuffle keys are {}",
        forceUpload, localStorage.getDiskSize(), localStorage.getShuffleMetaSet());

    List<ShuffleFileInfo> shuffleFileInfos = selectShuffleFiles(uploadThreadNum, forceUpload);
    if (shuffleFileInfos == null || shuffleFileInfos.isEmpty()) {
      cleanUploadedShuffle(Sets.newHashSet());
      return;
    }

    List<Callable<ShuffleUploadResult>> callableList = Lists.newLinkedList();
    List<String> shuffleKeys = Lists.newArrayList();
    long totalSize = 0;
    long maxSize = 0;
    for (ShuffleFileInfo shuffleFileInfo : shuffleFileInfos) {
      if (!shuffleFileInfo.isValid()) {
        continue;
      }

      if (forceUpload) {
        boolean locked = localStorage.lockShuffleExcluded(shuffleFileInfo.getKey());
        if (!locked) {
          continue;
        }
        shuffleKeys.add(shuffleFileInfo.getKey());
      }
      totalSize = totalSize + shuffleFileInfo.getSize();
      maxSize = Math.max(maxSize, shuffleFileInfo.getSize());
      Callable<ShuffleUploadResult> callable = () -> {
        try {
          CreateShuffleUploadHandlerRequest request =
              new CreateShuffleUploadHandlerRequest.Builder()
                  .remoteStorageType(remoteStorageType)
                  .remoteStorageBasePath(
                      ShuffleStorageUtils.getFullShuffleDataFolder(hdfsBasePath, shuffleFileInfo.getKey()))
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
        callableList.size(), maxSize, totalSize, uploadTimeoutS, localStorage.getDiskSize());
    long startTimeMs = System.currentTimeMillis();
    try {
      Set<String> successUploadShuffles  = Sets.newHashSet();
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
            localStorage.removeShuffle(shuffleUploadResult.getShuffleKey(), shuffleUploadResult.getSize(),
                shuffleUploadResult.getPartitions());
          } else {
            String shuffleKey = shuffleUploadResult.getShuffleKey();
            localStorage.updateUploadedShuffle(shuffleKey, shuffleUploadResult.getSize(),
                shuffleUploadResult.getPartitions());
            if (localStorage.getNotUploadedPartitions(shuffleKey).isEmpty()) {
              successUploadShuffles.add(shuffleKey);
            }
          }
        } else {
          future.cancel(true);
        }
      }
      cleanUploadedShuffle(successUploadShuffles);
    } catch (Exception e) {
      LOG.error(
          "Fail to upload {}, {}",
          shuffleFileInfos.stream().map(ShuffleFileInfo::getKey).collect(Collectors.joining("\n")),
          ExceptionUtils.getStackTrace(e));
    } finally {
      for (String shuffleKey : shuffleKeys) {
        localStorage.unlockShuffleExcluded(shuffleKey);
      }
      LOG.info("{} upload use {}ms and disk size is {}",
          Thread.currentThread().getName(), System.currentTimeMillis() - startTimeMs, localStorage.getDiskSize());
    }
  }

  private void deleteForceUploadPartitions(String shuffleKey, List<Integer> partitions) {
    int failDeleteFiles = 0;
    for (int partition : partitions) {
      String filePrefix = ShuffleStorageUtils.generateAbsoluteFilePrefix(
          localStorage.getBasePath(), shuffleKey, partition, serverId);
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
    List<String> shuffleKeys = localStorage.getSortedShuffleKeys(!forceUpload, num);
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
    RoaringBitmap bitmap = localStorage.getNotUploadedPartitions(key);
    List<Integer> partitionList = Lists.newArrayList();
    for (int p : bitmap) {
      partitionList.add(p);
    }
    partitionList.sort(Integer::compare);
    return partitionList;
  }

  private long getNotUploadedSize(String key) {
    return localStorage.getNotUploadedSize(key);
  }

  @VisibleForTesting
  ShuffleUploadHandlerFactory getHandlerFactory() {
    return ShuffleUploadHandlerFactory.getInstance();
  }


  private long addPartition(ShuffleFileInfo shuffleFileInfo, int partition) {
      String filePrefix = ShuffleStorageUtils.generateAbsoluteFilePrefix(
          localStorage.getBasePath(), shuffleFileInfo.getKey(), partition, serverId);
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

  @VisibleForTesting
  void cleanUploadedShuffle(Set<String> successUploadShuffles) {
    Queue<String> expiredShuffleKeys = localStorage.getExpiredShuffleKeys();
    if (localStorage.getDiskSize() * 100.0 / localStorage.getCapacity() < cleanupThreshold) {
      return;
    }

    for (String key = expiredShuffleKeys.poll(); key != null; key = expiredShuffleKeys.poll()) {
      successUploadShuffles.add(key);
    }

    successUploadShuffles.forEach((shuffleKey) -> {
      // If shuffle data is started to read, shuffle data won't be appended. When shuffle is
      // uploaded totally, the partitions which is not uploaded is empty.
      if (localStorage.isShuffleLongTimeNotRead(shuffleKey)) {
        String shufflePath = ShuffleStorageUtils.getFullShuffleDataFolder(localStorage.getBasePath(), shuffleKey);
        long start = System.currentTimeMillis();
        try {
          File baseFolder = new File(shufflePath);
          FileUtils.deleteDirectory(baseFolder);
          LOG.info("Clean shuffle {}", shuffleKey);
          localStorage.removeResources(shuffleKey);
          LOG.info("Delete shuffle data for shuffle [" + shuffleKey + "] with " + shufflePath
              + " cost " + (System.currentTimeMillis() - start) + " ms");
        } catch (Exception e) {
          LOG.warn("Can't delete shuffle data for shuffle [" + shuffleKey + "] with " + shufflePath, e);
          expiredShuffleKeys.add(shuffleKey);
        }
      } else {
          expiredShuffleKeys.add(shuffleKey);
      }
    });
  }
}
