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

package org.apache.tez.runtime.library.common.sort.impl;

import java.io.IOException;
import java.util.zip.Checksum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class RssTezPerPartitionRecord extends TezSpillRecord {
  private int numPartitions;
  private int[] numRecordsPerPartition;

  public RssTezPerPartitionRecord(int numPartitions) {
    super(numPartitions);
    this.numPartitions = numPartitions;
  }

  public RssTezPerPartitionRecord(int numPartitions, int[] numRecordsPerPartition) {
    super(numPartitions);
    this.numPartitions = numPartitions;
    this.numRecordsPerPartition = numRecordsPerPartition;
  }

  public RssTezPerPartitionRecord(Path indexFileName, Configuration job) throws IOException {
    super(indexFileName, job);
  }

  public RssTezPerPartitionRecord(Path indexFileName, Configuration job, String expectedIndexOwner)
      throws IOException {
    super(indexFileName, job, expectedIndexOwner);
  }

  public RssTezPerPartitionRecord(
      Path indexFileName, Configuration job, Checksum crc, String expectedIndexOwner)
      throws IOException {
    super(indexFileName, job, crc, expectedIndexOwner);
  }

  @Override
  public int size() {
    return numPartitions;
  }

  @Override
  public RssTezIndexRecord getIndex(int i) {
    int records = numRecordsPerPartition[i];
    RssTezIndexRecord rssTezIndexRecord = new RssTezIndexRecord();
    rssTezIndexRecord.setData(!(records == 0));
    return rssTezIndexRecord;
  }

  static class RssTezIndexRecord extends TezIndexRecord {
    private boolean hasData;

    private void setData(boolean hasData) {
      this.hasData = hasData;
    }

    @Override
    public boolean hasData() {
      return hasData;
    }
  }
}
