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

package com.tencent.rss.client.util;

import com.tencent.rss.common.util.Constants;

public class ClientUtils {

  // BlockId is long and composed by partitionId, executorId and AtomicInteger
  // AtomicInteger is first 19 bit, max value is 2^19 - 1
  // partitionId is next 24 bit, max value is 2^24 - 1
  // taskAttemptId is rest of 20 bit, max value is 2^20 - 1
  public static Long getBlockId(long partitionId, long taskAttemptId, long atomicInt) {
    if (atomicInt < 0 || atomicInt > Constants.MAX_SEQUENCE_NO) {
      throw new RuntimeException("Can't support sequence[" + atomicInt
          + "], the max value should be " + Constants.MAX_SEQUENCE_NO);
    }
    if (partitionId < 0 || partitionId > Constants.MAX_PARTITION_ID) {
      throw new RuntimeException("Can't support partitionId["
          + partitionId + "], the max value should be " + Constants.MAX_PARTITION_ID);
    }
    if (taskAttemptId < 0 || taskAttemptId > Constants.MAX_TASK_ATTEMPT_ID) {
      throw new RuntimeException("Can't support taskAttemptId["
          + taskAttemptId + "], the max value should be " + Constants.MAX_TASK_ATTEMPT_ID);
    }
    return (atomicInt << (Constants.PARTITION_ID_MAX_LENGTH + Constants.TASK_ATTEMPT_ID_MAX_LENGTH))
        + (partitionId << Constants.TASK_ATTEMPT_ID_MAX_LENGTH) + taskAttemptId;
  }

  // blockId will be stored in bitmap in shuffle server, more bitmap will cost more memory
  // to reduce memory cost, merge blockId of different partition in one bitmap
  public static int getBitmapNum(
      int taskNum,
      int partitionNum,
      int blockNumPerTaskPerPartition,
      long blockNumPerBitmap) {
    // depend on performance test, spark.rss.block.per.bitmap should be great than 20000000
    if (blockNumPerBitmap < 20000000) {
      throw new IllegalArgumentException("blockNumPerBitmap should be greater than 20000000");
    }
    // depend on actual job, spark.rss.block.per.task.partition should be less than 1000000
    // which maybe generate about 1T shuffle data/per task per partition
    if (blockNumPerTaskPerPartition < 0 || blockNumPerTaskPerPartition > 1000000) {
      throw new IllegalArgumentException("blockNumPerTaskPerPartition should be less than 1000000");
    }
    // to avoid overflow when do the calculation, reduce the data if possible
    // it's ok the result is not accuracy
    int processedTaskNum = taskNum;
    int processedPartitionNum = partitionNum;
    long processedBlockNumPerBitmap = blockNumPerBitmap;
    if (taskNum > 1000) {
      processedTaskNum = taskNum / 1000;
      processedBlockNumPerBitmap = processedBlockNumPerBitmap / 1000;
    }
    if (partitionNum > 1000) {
      processedPartitionNum = partitionNum / 1000;
      processedBlockNumPerBitmap = processedBlockNumPerBitmap / 1000;
    }
    long bitmapNum = 1L * blockNumPerTaskPerPartition * processedTaskNum
        * processedPartitionNum / processedBlockNumPerBitmap + 1;
    if (bitmapNum > partitionNum || bitmapNum < 0) {
      bitmapNum = partitionNum;
    }
    return (int) bitmapNum;
  }
}
