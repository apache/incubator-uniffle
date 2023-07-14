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
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.util.IdHelper;

public class CreateShuffleReadClientRequest {

  private String appId;
  private int shuffleId;
  private int partitionId;
  private String basePath;
  private int partitionNumPerRange;
  private int partitionNum;
  private Roaring64NavigableMap blockIdBitmap;
  private Roaring64NavigableMap taskIdBitmap;
  private List<ShuffleServerInfo> shuffleServerInfoList;
  private Configuration hadoopConf;
  private IdHelper idHelper;
  private ShuffleDataDistributionType shuffleDataDistributionType =
      ShuffleDataDistributionType.NORMAL;
  private boolean expectedTaskIdsBitmapFilterEnable = false;
  private RssConf rssConf;

  public CreateShuffleReadClientRequest(
      String appId,
      int shuffleId,
      int partitionId,
      String basePath,
      int partitionNumPerRange,
      int partitionNum,
      Roaring64NavigableMap blockIdBitmap,
      Roaring64NavigableMap taskIdBitmap,
      List<ShuffleServerInfo> shuffleServerInfoList,
      Configuration hadoopConf,
      ShuffleDataDistributionType dataDistributionType,
      boolean expectedTaskIdsBitmapFilterEnable,
      RssConf rssConf) {
    this(
        appId,
        shuffleId,
        partitionId,
        basePath,
        partitionNumPerRange,
        partitionNum,
        blockIdBitmap,
        taskIdBitmap,
        shuffleServerInfoList,
        hadoopConf,
        new DefaultIdHelper(),
        expectedTaskIdsBitmapFilterEnable,
        rssConf);
    this.shuffleDataDistributionType = dataDistributionType;
  }

  public CreateShuffleReadClientRequest(
      String appId,
      int shuffleId,
      int partitionId,
      String basePath,
      int partitionNumPerRange,
      int partitionNum,
      Roaring64NavigableMap blockIdBitmap,
      Roaring64NavigableMap taskIdBitmap,
      List<ShuffleServerInfo> shuffleServerInfoList,
      Configuration hadoopConf,
      IdHelper idHelper,
      boolean expectedTaskIdsBitmapFilterEnable,
      RssConf rssConf) {
    this(
        appId,
        shuffleId,
        partitionId,
        basePath,
        partitionNumPerRange,
        partitionNum,
        blockIdBitmap,
        taskIdBitmap,
        shuffleServerInfoList,
        hadoopConf,
        idHelper,
        expectedTaskIdsBitmapFilterEnable);
    this.rssConf = rssConf;
  }

  public CreateShuffleReadClientRequest(
      String appId,
      int shuffleId,
      int partitionId,
      String basePath,
      int partitionNumPerRange,
      int partitionNum,
      Roaring64NavigableMap blockIdBitmap,
      Roaring64NavigableMap taskIdBitmap,
      List<ShuffleServerInfo> shuffleServerInfoList,
      Configuration hadoopConf,
      IdHelper idHelper,
      boolean expectedTaskIdsBitmapFilterEnable) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.basePath = basePath;
    this.partitionNumPerRange = partitionNumPerRange;
    this.partitionNum = partitionNum;
    this.blockIdBitmap = blockIdBitmap;
    this.taskIdBitmap = taskIdBitmap;
    this.shuffleServerInfoList = shuffleServerInfoList;
    this.hadoopConf = hadoopConf;
    this.idHelper = idHelper;
    this.expectedTaskIdsBitmapFilterEnable = expectedTaskIdsBitmapFilterEnable;
  }

  public CreateShuffleReadClientRequest(
      String appId,
      int shuffleId,
      int partitionId,
      String basePath,
      int partitionNumPerRange,
      int partitionNum,
      Roaring64NavigableMap blockIdBitmap,
      Roaring64NavigableMap taskIdBitmap,
      List<ShuffleServerInfo> shuffleServerInfoList,
      Configuration hadoopConf,
      boolean expectedTaskIdsBitmapFilterEnable,
      RssConf rssConf) {
    this(
        appId,
        shuffleId,
        partitionId,
        basePath,
        partitionNumPerRange,
        partitionNum,
        blockIdBitmap,
        taskIdBitmap,
        shuffleServerInfoList,
        hadoopConf,
        new DefaultIdHelper(),
        expectedTaskIdsBitmapFilterEnable);
    this.rssConf = rssConf;
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

  public String getBasePath() {
    return basePath;
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

  public boolean isExpectedTaskIdsBitmapFilterEnable() {
    return expectedTaskIdsBitmapFilterEnable;
  }

  public RssConf getRssConf() {
    return rssConf;
  }
}
