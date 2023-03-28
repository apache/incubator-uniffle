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

package org.apache.spark.shuffle.writer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.client.impl.ShuffleWriteClientImpl;
import org.apache.uniffle.client.response.SendShuffleDataResult;
import org.apache.uniffle.common.ShuffleBlockInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DataPusherTest {

  static class FakedShuffleWriteClient extends ShuffleWriteClientImpl {
    private SendShuffleDataResult fakedShuffleDataResult;

    FakedShuffleWriteClient() {
      this(
          "GRPC",
          1,
          1,
          10,
          1,
          1,
          1,
          false,
          1,
          1,
          1,
          1
      );
    }

    private FakedShuffleWriteClient(String clientType, int retryMax, long retryIntervalMax, int heartBeatThreadNum,
        int replica, int replicaWrite, int replicaRead, boolean replicaSkipEnabled, int dataTransferPoolSize,
        int dataCommitPoolSize, int unregisterThreadPoolSize, int unregisterRequestTimeSec) {
      super(clientType, retryMax, retryIntervalMax, heartBeatThreadNum, replica, replicaWrite, replicaRead,
          replicaSkipEnabled, dataTransferPoolSize, dataCommitPoolSize, unregisterThreadPoolSize,
          unregisterRequestTimeSec);
    }

    @Override
    public SendShuffleDataResult sendShuffleData(String appId, List<ShuffleBlockInfo> shuffleBlockInfoList,
        Supplier<Boolean> needCancelRequest) {
      return fakedShuffleDataResult;
    }

    public void setFakedShuffleDataResult(SendShuffleDataResult fakedShuffleDataResult) {
      this.fakedShuffleDataResult = fakedShuffleDataResult;
    }
  }

  @Test
  public void testSendData() throws ExecutionException, InterruptedException {
    FakedShuffleWriteClient shuffleWriteClient = new FakedShuffleWriteClient();

    Map<String, Set<Long>> taskToSuccessBlockIds = Maps.newConcurrentMap();
    Map<String, Set<Long>> taskToFailedBlockIds = Maps.newConcurrentMap();
    Set<String> failedTaskIds = new HashSet<>();

    DataPusher dataPusher = new DataPusher(
        shuffleWriteClient,
        taskToSuccessBlockIds,
        taskToFailedBlockIds,
        failedTaskIds,
        1,
        2
    );
    dataPusher.setRssAppId("testSendData_appId");

    // sync send
    AddBlockEvent event = new AddBlockEvent("taskId", Arrays.asList(
        new ShuffleBlockInfo(
            1, 1, 1, 1, 1, new byte[1], null, 1, 100, 1
        ))
    );
    shuffleWriteClient.setFakedShuffleDataResult(
        new SendShuffleDataResult(
            Sets.newHashSet(1L, 2L),
            Sets.newHashSet(3L, 4L)
        )
    );
    CompletableFuture<Long> future = dataPusher.send(event);
    long memoryFree = future.get();
    assertEquals(100, memoryFree);
    assertTrue(taskToSuccessBlockIds.get("taskId").contains(1L));
    assertTrue(taskToSuccessBlockIds.get("taskId").contains(2L));
    assertTrue(taskToFailedBlockIds.get("taskId").contains(3L));
    assertTrue(taskToFailedBlockIds.get("taskId").contains(4L));
  }
}
