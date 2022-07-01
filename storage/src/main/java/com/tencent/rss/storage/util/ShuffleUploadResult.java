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

package com.tencent.rss.storage.util;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class ShuffleUploadResult {
  private String shuffleKey;
  private long size;
  private List<Integer> partitions;

  public ShuffleUploadResult() {
    this.size = 0;
    partitions = null;
  }

  public ShuffleUploadResult(long size, List<Integer> partitions) {
    this.size = size;
    this.partitions = partitions;
  }

  public String getShuffleKey() {
    return shuffleKey;
  }

  public void setShuffleKey(String shuffleKey) {
    this.shuffleKey = shuffleKey;
  }

  public long getSize() {
    return size;
  }

  public void setSize(long size) {
    this.size = size;
  }

  public List<Integer> getPartitions() {
    return partitions;
  }

  public void setPartitions(List<Integer> partitions) {
    this.partitions = partitions;
  }

  public static ShuffleUploadResult merge(List<ShuffleUploadResult> results) {
    if (results == null || results.isEmpty()) {
      return null;
    }

    long size = results.stream().map(ShuffleUploadResult::getSize).reduce(0L, Long::sum);
    List<Integer> partitions = results
        .stream()
        .map(ShuffleUploadResult::getPartitions)
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
    return new ShuffleUploadResult(size, partitions);
  }

}
