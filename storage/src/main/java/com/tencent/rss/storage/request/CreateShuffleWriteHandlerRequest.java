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

package com.tencent.rss.storage.request;

import org.apache.hadoop.conf.Configuration;

public class CreateShuffleWriteHandlerRequest {

  private String storageType;
  private String appId;
  private int shuffleId;
  private int startPartition;
  private int endPartition;
  private String[] storageBasePaths;
  private String fileNamePrefix;
  private Configuration conf;
  private int storageDataReplica;

  public CreateShuffleWriteHandlerRequest(
      String storageType,
      String appId,
      int shuffleId,
      int startPartition,
      int endPartition,
      String[] storageBasePaths,
      String fileNamePrefix,
      Configuration conf,
      int storageDataReplica) {
    this.storageType = storageType;
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.startPartition = startPartition;
    this.endPartition = endPartition;
    this.storageBasePaths = storageBasePaths;
    this.fileNamePrefix = fileNamePrefix;
    this.conf = conf;
    this.storageDataReplica = storageDataReplica;
  }

  public String getStorageType() {
    return storageType;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getStartPartition() {
    return startPartition;
  }

  public int getEndPartition() {
    return endPartition;
  }

  public String[] getStorageBasePaths() {
    return storageBasePaths;
  }

  public String getFileNamePrefix() {
    return fileNamePrefix;
  }

  public Configuration getConf() {
    return conf;
  }

  public int getStorageDataReplica() {
    return storageDataReplica;
  }
}
