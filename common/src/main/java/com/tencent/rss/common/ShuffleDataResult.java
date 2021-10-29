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

package com.tencent.rss.common;

import java.util.List;

public class ShuffleDataResult {

  private byte[] data;
  private List<BufferSegment> bufferSegments;

  public ShuffleDataResult() {
    this.bufferSegments = null;
  }

  public ShuffleDataResult(byte[] data, List<BufferSegment> bufferSegments) {
    this.data = data;
    this.bufferSegments = bufferSegments;
  }

  public byte[] getData() {
    return data;
  }

  public List<BufferSegment> getBufferSegments() {
    return bufferSegments;
  }

  public boolean isEmpty() {
    return bufferSegments == null || bufferSegments.isEmpty() || data == null || data.length == 0;
  }

}
