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

package org.apache.uniffle.client.request;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.util.DefaultIdHelper;
import org.apache.uniffle.client.util.IdHelper;
import org.apache.uniffle.common.BlockSkipStrategy;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;

public class CreateShuffleReadClientRequest {

  private String appId;
  private int shuffleId;
  private int partitionId;
  private String storageType;
  private String basePath;
  private int indexReadLimit;
  private int readBufferSize;
  private int partitionNumPerRange;
  private int partitionNum;
  private Roaring64NavigableMap blockIdBitmap;
  private Roaring64NavigableMap taskIdBitmap;
  private List<ShuffleServerInfo> shuffleServerInfoList;
  private Configuration hadoopConf;
  private IdHelper idHelper;
  private ShuffleDataDistributionType shuffleDataDistributionType = ShuffleDataDistributionType.NORMAL;
  private BlockSkipStrategy blockSkipStrategy;
  private int maxBlockIdRangeSegments;

  public CreateShuffleReadClientRequest(
      String appId,
      int shuffleId,
      int partitionId,
      String storageType,
      String basePath,
      int indexReadLimit,
      int readBufferSize,
      int partitionNumPerRange,
      int partitionNum,
      Roaring64NavigableMap blockIdBitmap,
      Roaring64NavigableMap taskIdBitmap,
      List<ShuffleServerInfo> shuffleServerInfoList,
      Configuration hadoopConf,
      ShuffleDataDistributionType dataDistributionType,
      BlockSkipStrategy blockSkipStrategy,
      int maxBlockIdRangeSegments) {
    this(appId, shuffleId, partitionId, storageType, basePath, indexReadLimit, readBufferSize,
        partitionNumPerRange, partitionNum, blockIdBitmap, taskIdBitmap, shuffleServerInfoList,
        hadoopConf, new DefaultIdHelper(), blockSkipStrategy, maxBlockIdRangeSegments);
    this.shuffleDataDistributionType = dataDistributionType;
  }

  public CreateShuffleReadClientRequest(
      String appId,
      int shuffleId,
      int partitionId,
      String storageType,
      String basePath,
      int indexReadLimit,
      int readBufferSize,
      int partitionNumPerRange,
      int partitionNum,
      Roaring64NavigableMap blockIdBitmap,
      Roaring64NavigableMap taskIdBitmap,
      List<ShuffleServerInfo> shuffleServerInfoList,
      Configuration hadoopConf,
      BlockSkipStrategy blockSkipStrategy,
      int maxBlockIdRangeSegments) {
    this(appId, shuffleId, partitionId, storageType, basePath, indexReadLimit, readBufferSize,
        partitionNumPerRange, partitionNum, blockIdBitmap, taskIdBitmap, shuffleServerInfoList,
        hadoopConf, new DefaultIdHelper(), blockSkipStrategy, maxBlockIdRangeSegments);
  }

  public CreateShuffleReadClientRequest(
      String appId,
      int shuffleId,
      int partitionId,
      String storageType,
      String basePath,
      int indexReadLimit,
      int readBufferSize,
      int partitionNumPerRange,
      int partitionNum,
      Roaring64NavigableMap blockIdBitmap,
      Roaring64NavigableMap taskIdBitmap,
      List<ShuffleServerInfo> shuffleServerInfoList,
      Configuration hadoopConf,
      IdHelper idHelper,
      BlockSkipStrategy blockSkipStrategy,
      int maxBlockIdRangeSegments) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.storageType = storageType;
    this.basePath = basePath;
    this.indexReadLimit = indexReadLimit;
    this.readBufferSize = readBufferSize;
    this.partitionNumPerRange = partitionNumPerRange;
    this.partitionNum = partitionNum;
    this.blockIdBitmap = blockIdBitmap;
    this.taskIdBitmap = taskIdBitmap;
    this.shuffleServerInfoList = shuffleServerInfoList;
    this.hadoopConf = hadoopConf;
    this.idHelper = idHelper;
    this.blockSkipStrategy = blockSkipStrategy;
    this.maxBlockIdRangeSegments = maxBlockIdRangeSegments;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public int getPartitionNumPerRange() {
    return partitionNumPerRange;
  }

  public int getPartitionNum() {
    return partitionNum;
  }

  public String getStorageType() {
    return storageType;
  }

  public String getBasePath() {
    return basePath;
  }

  public int getIndexReadLimit() {
    return indexReadLimit;
  }

  public int getReadBufferSize() {
    return readBufferSize;
  }

  public Roaring64NavigableMap getBlockIdBitmap() {
    return blockIdBitmap;
  }

  public Roaring64NavigableMap getTaskIdBitmap() {
    return taskIdBitmap;
  }

  public List<ShuffleServerInfo> getShuffleServerInfoList() {
    return shuffleServerInfoList;
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  public IdHelper getIdHelper() {
    return idHelper;
  }

  public ShuffleDataDistributionType getShuffleDataDistributionType() {
    return shuffleDataDistributionType;
  }

  public int getMaxBlockIdRangeSegments() {
    return maxBlockIdRangeSegments;
  }

  public void setMaxBlockIdRangeSegments(int maxBlockIdRangeSegments) {
    this.maxBlockIdRangeSegments = maxBlockIdRangeSegments;
  }

  public BlockSkipStrategy getBlockSkipStrategy() {
    return blockSkipStrategy;
  }

  public void setBlockSkipStrategy(BlockSkipStrategy blockSkipStrategy) {
    this.blockSkipStrategy = blockSkipStrategy;
  }
}
