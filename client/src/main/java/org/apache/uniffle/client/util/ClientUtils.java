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

package org.apache.uniffle.client.util;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.storage.util.StorageType;

public class ClientUtils {

  // BlockId is long and composed by partitionId, executorId and AtomicInteger
  // AtomicInteger is first 19 bit, max value is 2^19 - 1
  // partitionId is next 24 bit, max value is 2^24 - 1
  // taskAttemptId is rest of 20 bit, max value is 2^20 - 1
  public static Long getBlockId(long partitionId, long taskAttemptId, long atomicInt) {
    if (atomicInt < 0 || atomicInt > Constants.MAX_SEQUENCE_NO) {
      throw new IllegalArgumentException("Can't support sequence[" + atomicInt
          + "], the max value should be " + Constants.MAX_SEQUENCE_NO);
    }
    if (partitionId < 0 || partitionId > Constants.MAX_PARTITION_ID) {
      throw new IllegalArgumentException("Can't support partitionId["
          + partitionId + "], the max value should be " + Constants.MAX_PARTITION_ID);
    }
    if (taskAttemptId < 0 || taskAttemptId > Constants.MAX_TASK_ATTEMPT_ID) {
      throw new IllegalArgumentException("Can't support taskAttemptId["
          + taskAttemptId + "], the max value should be " + Constants.MAX_TASK_ATTEMPT_ID);
    }
    return (atomicInt << (Constants.PARTITION_ID_MAX_LENGTH + Constants.TASK_ATTEMPT_ID_MAX_LENGTH))
        + (partitionId << Constants.TASK_ATTEMPT_ID_MAX_LENGTH) + taskAttemptId;
  }

  public static RemoteStorageInfo fetchRemoteStorage(
      String appId,
      RemoteStorageInfo defaultRemoteStorage,
      boolean dynamicConfEnabled,
      String storageType,
      ShuffleWriteClient shuffleWriteClient) {
    RemoteStorageInfo remoteStorage = defaultRemoteStorage;
    if (remoteStorage.isEmpty() && requireRemoteStorage(storageType)) {
      if (dynamicConfEnabled) {
        // get from coordinator first
        remoteStorage = shuffleWriteClient.fetchRemoteStorage(appId);
        if (remoteStorage.isEmpty()) {
          // empty from coordinator, use default remote storage
          remoteStorage = defaultRemoteStorage;
        }
      } else {
        remoteStorage = defaultRemoteStorage;
      }
      if (remoteStorage.isEmpty()) {
        throw new RuntimeException("Can't find remoteStorage: with storageType[" + storageType + "]");
      }
    }
    return remoteStorage;
  }

  private static boolean requireRemoteStorage(String storageType) {
    return StorageType.MEMORY_HDFS.name().equals(storageType)
        || StorageType.MEMORY_LOCALFILE_HDFS.name().equals(storageType)
        || StorageType.HDFS.name().equals(storageType)
        || StorageType.LOCALFILE_HDFS.name().equals(storageType);
  }
}
