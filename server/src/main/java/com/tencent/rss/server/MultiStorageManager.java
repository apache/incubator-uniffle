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

import com.google.common.collect.Lists;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.util.RssUtils;
import com.tencent.rss.storage.common.DiskItem;
import com.tencent.rss.storage.factory.ShuffleHandlerFactory;
import com.tencent.rss.storage.handler.api.ShuffleDeleteHandler;
import com.tencent.rss.storage.request.CreateShuffleDeleteHandlerRequest;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import com.tencent.rss.storage.util.StorageType;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;

public class MultiStorageManager {

  private static final Logger LOG = LoggerFactory.getLogger(MultiStorageManager.class);
  private final String[] dirs;
  private final long capacity;
  private final double cleanupThreshold;
  private final double highWaterMarkOfWrite;
  private final double lowWaterMarkOfWrite;

  // config for uploader
  private final boolean enableUploader;
  private final int uploadThreadNum;
  private final long uploadIntervalMS;
  private final long uploadCombineThresholdMB;
  private final long referenceUploadSpeedMBS;
  private final StorageType remoteStorageType;
  private final String hdfsBathPath;
  private final String shuffleServerId;
  private final Configuration hadoopConf;
  private final long cleanupIntervalMs;
  private final long maxShuffleSize;

  private final List<DiskItem> diskItems = Lists.newArrayList();
  private final List<ShuffleUploader> uploaders = Lists.newArrayList();
  private final long shuffleExpiredTimeoutMs;
  private final long maxForceUploadExpireTimeS;

  public MultiStorageManager(ShuffleServerConf conf, String shuffleServerId) {
    String dirsFromConf = conf.getString(ShuffleServerConf.RSS_STORAGE_BASE_PATH);
    if (dirsFromConf == null) {
      throw new IllegalArgumentException("Base path dirs must not be empty");
    }
    dirs = dirsFromConf.split(",");
    long capacity = conf.getSizeAsBytes(ShuffleServerConf.DISK_CAPACITY);
    if (capacity <= 0) {
      throw new IllegalArgumentException("Capacity must be larger than zero");
    }

    double cleanupThreshold = conf.get(ShuffleServerConf.CLEANUP_THRESHOLD);
    if (cleanupThreshold < 0 || cleanupThreshold > 100) {
      throw new IllegalArgumentException("cleanupThreshold must be between 0 and 100");
    }

    long cleanupIntervalMs  = conf.get(ShuffleServerConf.CLEANUP_INTERVAL_MS);
    if (cleanupIntervalMs < 0) {
      throw new IllegalArgumentException("cleanupInterval must be larger than zero");
    }

    double highWaterMarkOfWrite = conf.get(ShuffleServerConf.HIGH_WATER_MARK_OF_WRITE);
    double lowWaterMarkOfWrite = conf.get(ShuffleServerConf.LOW_WATER_MARK_OF_WRITE);

    if (highWaterMarkOfWrite < lowWaterMarkOfWrite) {
      throw new IllegalArgumentException("highWaterMarkOfWrite must be larger than lowWaterMarkOfWrite");
    }

    if (lowWaterMarkOfWrite < 0) {
      throw new IllegalArgumentException("lowWaterMarkOfWrite must be larger than zero");
    }

    if (highWaterMarkOfWrite > 100) {
      throw new IllegalArgumentException("highWaterMarkOfWrite must be smaller than 100");
    }

    // todo: implement a method in class Config like `checkValue`
    int uploadThreadNum = conf.get(ShuffleServerConf.UPLOADER_THREAD_NUM);
    if (uploadThreadNum <= 0) {
      throw new IllegalArgumentException("uploadThreadNum must be larger than 0");
    }

    long uploadIntervalMS = conf.get(ShuffleServerConf.UPLOADER_INTERVAL_MS);
    if (uploadIntervalMS <= 0) {
      throw new IllegalArgumentException("uploadIntervalMs must be larger than 0");
    }

    long uploadCombineThresholdMB = conf.get(ShuffleServerConf.UPLOAD_COMBINE_THRESHOLD_MB);
    if (uploadCombineThresholdMB <= 0) {
      throw new IllegalArgumentException("uploadCombineThresholdMB must be larger than 0");
    }

    long referenceUploadSpeedMBS = conf.get(ShuffleServerConf.REFERENCE_UPLOAD_SPEED_MBS);
    if (referenceUploadSpeedMBS <= 0) {
      throw new IllegalArgumentException("referenceUploadSpeedMbps must be larger than 0");
    }

    // todo: better name
    String hdfsBasePath = conf.get(ShuffleServerConf.HDFS_BASE_PATH);
    if (StringUtils.isEmpty(hdfsBasePath)) {
      throw new IllegalArgumentException("hdfsBasePath couldn't be empty");
    }

    StorageType remoteStorageType = StorageType.valueOf(conf.getString(ShuffleServerConf.UPLOAD_STORAGE_TYPE));

    if (StorageType.LOCALFILE.equals(remoteStorageType)) {
      throw new IllegalArgumentException("uploadRemoteStorageType couldn't be LOCALFILE");
    }

    long shuffleExpiredTimeoutMs = conf.get(ShuffleServerConf.SHUFFLE_EXPIRED_TIMEOUT_MS);
    if (shuffleExpiredTimeoutMs <= 0) {
      throw new IllegalArgumentException("The value of shuffleExpiredTimeMs must be positive");
    }

    long maxShuffleSize = conf.get(ShuffleServerConf.SHUFFLE_MAX_UPLOAD_SIZE);
    if (maxShuffleSize <= 0) {
      throw new IllegalArgumentException("The value of maxShuffleSize must be positive");
    }

    long pendingEventTimeoutS = conf.get(ShuffleServerConf.PENDING_EVENT_TIMEOUT_SEC);
    if (pendingEventTimeoutS <= 0) {
      throw new IllegalArgumentException("The value of pendingEventTimeoutS must be positive");
    }

    double maxShuffleForceUploadTimeRatio = conf.get(ShuffleServerConf.SHUFFLE_MAX_FORCE_UPLOAD_TIME_RATIO);
    if (Double.compare(maxShuffleForceUploadTimeRatio, 0.0) <= 0
        || Double.compare(maxShuffleForceUploadTimeRatio, 100.0) >= 0) {
      throw new IllegalArgumentException("The value of maxShuffleForceUploadTimeRatio must be in (0, 100)");
    }

    this.enableUploader = conf.get(ShuffleServerConf.UPLOADER_ENABLE);
    this.capacity = capacity;
    this.cleanupThreshold = cleanupThreshold;
    this.cleanupIntervalMs = cleanupIntervalMs;
    this.highWaterMarkOfWrite = highWaterMarkOfWrite;
    this.lowWaterMarkOfWrite = lowWaterMarkOfWrite;
    this.uploadThreadNum = uploadThreadNum;
    this.uploadIntervalMS = uploadIntervalMS;
    this.uploadCombineThresholdMB = uploadCombineThresholdMB;
    this.referenceUploadSpeedMBS = referenceUploadSpeedMBS;
    this.remoteStorageType = remoteStorageType;
    this.hdfsBathPath = hdfsBasePath;
    this.shuffleServerId = shuffleServerId;
    this.hadoopConf = new Configuration();
    this.shuffleExpiredTimeoutMs = shuffleExpiredTimeoutMs;
    this.maxShuffleSize = maxShuffleSize;

    this.maxForceUploadExpireTimeS = (long) (pendingEventTimeoutS * (maxShuffleForceUploadTimeRatio / 100.0));
    if (maxForceUploadExpireTimeS <= 0) {
      throw new IllegalArgumentException("The value of maxForceUploadExpireTimeS must be positive");
    }

    // todo: extract a method
    for (String key : conf.getKeySet()) {
      if (key.startsWith(ShuffleServerConf.PREFIX_HADOOP_CONF)) {
        String value = conf.getString(key, "");
        String hadoopKey = key.substring(ShuffleServerConf.PREFIX_HADOOP_CONF.length() + 1);
        LOG.info("Update hadoop configuration:" + hadoopKey + "=" + value);
        hadoopConf.set(hadoopKey, value);
      }
    }
    initialize();
  }

  // remove initialize method
  void initialize() throws RuntimeException {
    // TODO: 1.adapt to heterogeneous env and config different capacity for each disk item
    //       2.each total capacity and server buffer size,
    for (String dir : dirs) {
      // todo: if there is a disk is corrupted, we should skip. now shuffleServer will
      // crash.
      DiskItem item = DiskItem.newBuilder().basePath(dir)
          .cleanupThreshold(cleanupThreshold)
          .highWaterMarkOfWrite(highWaterMarkOfWrite)
          .lowWaterMarkOfWrite(lowWaterMarkOfWrite)
          .capacity(capacity)
          .cleanIntervalMs(cleanupIntervalMs)
          .shuffleExpiredTimeoutMs(shuffleExpiredTimeoutMs)
          .build();
      diskItems.add(item);
    }

    if (enableUploader) {
      for (DiskItem item : diskItems) {
        ShuffleUploader shuffleUploader = new ShuffleUploader.Builder()
            .diskItem(item)
            .uploadThreadNum(uploadThreadNum)
            .uploadIntervalMS(uploadIntervalMS)
            .uploadCombineThresholdMB(uploadCombineThresholdMB)
            .referenceUploadSpeedMBS(referenceUploadSpeedMBS)
            .remoteStorageType(remoteStorageType)
            .hdfsBathPath(hdfsBathPath)
            .serverId(shuffleServerId)
            .hadoopConf(hadoopConf)
            .maxShuffleSize(maxShuffleSize)
            .maxForceUploadExpireTimeS(maxForceUploadExpireTimeS)
            .build();
        uploaders.add(shuffleUploader);
      }
    }
  }

  public void start() {
    for (DiskItem item : diskItems) {
      item.start();
    }
    for (ShuffleUploader uploader : uploaders) {
      uploader.start();
    }
  }

  public void stop() {
     for (DiskItem item : diskItems) {
       item.stop();
     }
     for (ShuffleUploader uploader : uploaders) {
       uploader.stop();
     }
  }

  public boolean canWrite(ShuffleDataFlushEvent event) {
    DiskItem diskItem = getDiskItem(event);
    return diskItem.canWrite();
  }

  public void updateWriteEvent(ShuffleDataFlushEvent event) {
    DiskItem diskItem = getDiskItem(event);
    String appId = event.getAppId();
    int shuffleId = event.getShuffleId();
    String key = RssUtils.generateShuffleKey(appId, shuffleId);
    List partitionList = Lists.newArrayList();
    for (int i = event.getStartPartition(); i <= event.getEndPartition(); i++) {
      partitionList.add(i);
    }
    long size = 0;
    for (ShufflePartitionedBlock block : event.getShuffleBlocks()) {
      size += block.getLength();
    }
    diskItem.updateWrite(key, size, partitionList);
  }

  public void prepareStartRead(String appId, int shuffleId, int partitionId) {
    DiskItem diskItem = getDiskItem(appId, shuffleId, partitionId);
    String key = RssUtils.generateShuffleKey(appId, shuffleId);
    diskItem.prepareStartRead(key);
  }

  public void updateLastReadTs(String appId, int shuffleId, int partitionId) {
    DiskItem diskItem = getDiskItem(appId, shuffleId, partitionId);
    String key = RssUtils.generateShuffleKey(appId, shuffleId);
    diskItem.updateShuffleLastReadTs(key);
  }

  public DiskItem getDiskItem(ShuffleDataFlushEvent event) {
    return getDiskItem(event.getAppId(), event.getShuffleId(), event.getStartPartition());
  }

  public DiskItem getDiskItem(String appId, int shuffleId, int partitionId) {
    int dirId = getDiskItemId(appId, shuffleId, partitionId);
    return diskItems.get(dirId);
  }

  public int getDiskItemId(String appId, int shuffleId, int partitionId) {
    return ShuffleStorageUtils.getStorageIndex(diskItems.size(), appId, shuffleId, partitionId);
  }

  public void removeResources(String appId, Set<Integer> shuffleSet) {
    LOG.info("Start to remove resource of appId: {}, shuffles: {}", appId, shuffleSet.toString());
    ShuffleDeleteHandler deleteHandler = ShuffleHandlerFactory.getInstance()
        .createShuffleDeleteHandler(new CreateShuffleDeleteHandlerRequest("HDFS", hadoopConf));
    deleteHandler.delete(new String[] {hdfsBathPath}, appId);
    for (Integer shuffleId : shuffleSet) {
      diskItems.forEach(item -> item.addExpiredShuffleKey(
          RssUtils.generateShuffleKey(appId, shuffleId)));
    }
  }

  public void createMetadataIfNotExist(ShuffleDataFlushEvent event) {
    DiskItem diskItem = getDiskItem(event);
    String appId = event.getAppId();
    int shuffleId = event.getShuffleId();
    String key = RssUtils.generateShuffleKey(appId, shuffleId);
    diskItem.createMetadataIfNotExist(key);
  }

  public ReadWriteLock getForceUploadLock(ShuffleDataFlushEvent event) {
    DiskItem diskItem = getDiskItem(event);
    String appId = event.getAppId();
    int shuffleId = event.getShuffleId();
    String key = RssUtils.generateShuffleKey(appId, shuffleId);
    return diskItem.getLock(key);
  }
}
