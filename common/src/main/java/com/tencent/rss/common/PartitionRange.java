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

import java.util.Objects;

/**
 * Class for partition range: [start, end]
 * Note: both inclusive
 */
public class PartitionRange implements Comparable<PartitionRange> {

  private final int start;
  private final int end;

  public PartitionRange(int start, int end) {
    if (start > end || start < 0) {
      throw new IllegalArgumentException("Illegal partition range [start, end]");
    }
    this.start = start;
    this.end = end;
  }

  public int getStart() {
    return start;
  }

  public int getEnd() {
    return end;
  }

  public int getPartitionNum() {
    return end - start + 1;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PartitionRange that = (PartitionRange) o;
    return start == that.start && end == that.end;
  }

  @Override
  public int hashCode() {
    return Objects.hash(start, end);
  }

  @Override
  public String toString() {
    return "PartitionRange[" + start + ", " + end + ']';
  }

  @Override
  public int compareTo(PartitionRange o) {
    return this.getStart() - o.getStart();
  }
}
