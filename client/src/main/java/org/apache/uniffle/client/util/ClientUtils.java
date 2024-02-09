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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.storage.util.StorageType;

public class ClientUtils {

  // BlockId is positive long (63 bits) composed of partitionId, taskAttemptId and AtomicInteger.
  // AtomicInteger is highest 18 bits, max value is 2^18 - 1
  // partitionId is middle 24 bits, max value is 2^24 - 1
  // taskAttemptId is lowest 21 bits, max value is 2^21 - 1
  // Values of partitionId, taskAttemptId and AtomicInteger are always positive.
  public static Long getBlockId(long partitionId, long taskAttemptId, long atomicInt) {
    if (atomicInt < 0 || atomicInt > Constants.MAX_SEQUENCE_NO) {
      throw new IllegalArgumentException(
          "Can't support sequence["
              + atomicInt
              + "], the max value should be "
              + Constants.MAX_SEQUENCE_NO);
    }
    if (partitionId < 0 || partitionId > Constants.MAX_PARTITION_ID) {
      throw new IllegalArgumentException(
          "Can't support partitionId["
              + partitionId
              + "], the max value should be "
              + Constants.MAX_PARTITION_ID);
    }
    if (taskAttemptId < 0 || taskAttemptId > Constants.MAX_TASK_ATTEMPT_ID) {
      throw new IllegalArgumentException(
          "Can't support taskAttemptId["
              + taskAttemptId
              + "], the max value should be "
              + Constants.MAX_TASK_ATTEMPT_ID);
    }
    return (atomicInt << (Constants.PARTITION_ID_MAX_LENGTH + Constants.TASK_ATTEMPT_ID_MAX_LENGTH))
        + (partitionId << Constants.TASK_ATTEMPT_ID_MAX_LENGTH)
        + taskAttemptId;
  }

  public static RemoteStorageInfo fetchRemoteStorage(
      String appId,
      RemoteStorageInfo defaultRemoteStorage,
      boolean dynamicConfEnabled,
      String storageType,
      ShuffleWriteClient shuffleWriteClient) {
    RemoteStorageInfo remoteStorage = defaultRemoteStorage;
    if (storageType != null && StorageType.withRemoteStorage(StorageType.valueOf(storageType))) {
      if (remoteStorage.isEmpty() && dynamicConfEnabled) {
        // fallback to dynamic conf on coordinator
        remoteStorage = shuffleWriteClient.fetchRemoteStorage(appId);
      }
      if (remoteStorage.isEmpty()) {
        throw new IllegalStateException(
            "Can't find remoteStorage: with storageType[" + storageType + "]");
      }
    }
    return remoteStorage;
  }

  @SuppressWarnings("rawtypes")
  public static boolean waitUntilDoneOrFail(
      List<CompletableFuture<Boolean>> futures, boolean allowFastFail) {
    int expected = futures.size();
    int failed = 0;

    CompletableFuture allFutures =
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

    List<Future> finished = new ArrayList<>();
    while (true) {
      for (Future<Boolean> future : futures) {
        if (future.isDone() && !finished.contains(future)) {
          finished.add(future);
          try {
            if (!future.get()) {
              failed++;
            }
          } catch (Exception e) {
            failed++;
          }
        }
      }

      if (expected == finished.size()) {
        return failed <= 0;
      }

      if (failed > 0 && allowFastFail) {
        futures.stream().filter(x -> !x.isDone()).forEach(x -> x.cancel(true));
        return false;
      }

      try {
        allFutures.get(10, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        // ignore
      }
    }
  }

  public static void validateTestModeConf(boolean testMode, String storageType) {
    if (!testMode
        && (StorageType.LOCALFILE.name().equals(storageType)
            || (StorageType.HDFS.name()).equals(storageType))) {
      throw new IllegalArgumentException(
          "LOCALFILE or HADOOP storage type should be used in test mode only, "
              + "because of the poor performance of these two types.");
    }
  }

  public static void validateClientType(String clientType) {
    Set<String> types =
        Arrays.stream(ClientType.values()).map(Enum::name).collect(Collectors.toSet());
    if (!types.contains(clientType)) {
      throw new IllegalArgumentException(
          String.format("The value of %s should be one of %s", clientType, types));
    }
  }
}
