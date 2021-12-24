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

import java.io.File;
import java.util.List;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.common.util.ByteUnit;

public class ShuffleFileInfo {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleFileInfo.class);

  private final List<File> dataFiles = Lists.newLinkedList();
  private final List<File> indexFiles = Lists.newLinkedList();
  private final List<Integer> partitions = Lists.newLinkedList();
  private String key;
  private long size;

  public boolean isValid() {
    if (key == null || key.isEmpty()) {
      LOG.error("Shuffle key is null or empty");
      return false;
    }

    if (size <= 0) {
      LOG.error("Total size of shuffle [{}]", key);
      return false;
    }

    if (dataFiles.isEmpty() || indexFiles.isEmpty() || partitions.isEmpty()) {
      LOG.error(
          "Data files num {}, index files num {} and partition files num {} is invalid",
          dataFiles.size(),
          indexFiles.size(),
          partitions.size());
      return false;
    }

    if ((dataFiles.size() != indexFiles.size()) || (dataFiles.size() != partitions.size())) {
      LOG.error(
          "Data files num {}, index files num {} and partition files num {} are not the same",
          dataFiles.size(),
          indexFiles.size(),
          partitions.size());
      return false;
    }

    return true;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public void setSize(long size) {
    this.size = size;
  }

  public List<File> getDataFiles() {
    return dataFiles;
  }

  public List<File> getIndexFiles() {
    return indexFiles;
  }

  public List<Integer> getPartitions() {
    return partitions;
  }



  public long getSize() {
    return size;
  }

  public boolean shouldCombine(long uploadCombineThresholdMB) {
    return ByteUnit.BYTE.toMiB(size / dataFiles.size()) < uploadCombineThresholdMB;
  }

  public boolean isEmpty() {
    return dataFiles.isEmpty();
  }

  public String getKey() {
    return key;
  }

}
