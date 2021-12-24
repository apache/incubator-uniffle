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

import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;

import com.tencent.rss.common.BufferSegment;

public class DataFileSegment extends FileSegment {

  private List<BufferSegment> bufferSegments;

  public DataFileSegment(String path, long offset, int length, List<BufferSegment> bufferSegments) {
    super(path, offset, length);
    this.bufferSegments = bufferSegments;
  }

  public List<BufferSegment> getBufferSegments() {
    return bufferSegments;
  }

  public Set<Long> getBlockIds() {
    Set<Long> blockIds = Sets.newHashSet();
    for (BufferSegment bs : bufferSegments) {
      blockIds.add(bs.getBlockId());
    }
    return blockIds;
  }
}
