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

package com.tencent.rss.storage.handler.impl;

import com.google.common.collect.Lists;
import com.tencent.rss.storage.util.ShuffleStorageUtils;

import java.util.List;

public class ShuffleIndexHeader {

  private int partitionNum;
  private final List<Entry> indexes = Lists.newArrayList();
  private long crc;

  public void setPartitionNum(int partitionNum) {
    this.partitionNum = partitionNum;
  }

  public int getPartitionNum() {
    return partitionNum;
  }

  public List<Entry> getIndexes() {
    return indexes;
  }

  public long getCrc() {
    return crc;
  }

  public void setCrc(long crc) {
    this.crc = crc;
  }

  public int getHeaderLen() {
    return (int)ShuffleStorageUtils.getIndexFileHeaderLen(partitionNum);
  }

  static class Entry {
    Integer partitionId;
    Long partitionIndexLength;
    Long partitionDataLength;

    Entry(Integer partitionId, Long partitionIndexLength, long partitionDataLength) {
      this.partitionId = partitionId;
      this.partitionIndexLength = partitionIndexLength;
      this.partitionDataLength = partitionDataLength;
    }

    public Integer getPartitionId() {
      return partitionId;
    }

    public Long getPartitionIndexLength() {
      return partitionIndexLength;
    }

    public Long getPartitionDataLength() {
      return partitionDataLength;
    }
  }
}
