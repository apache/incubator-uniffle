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

package org.apache.uniffle.client.impl.grpc;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.google.common.collect.Lists;
import io.grpc.ClientInterceptor;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.client.request.RssRegisterShuffleRequest;
import org.apache.uniffle.client.retry.DefaultBackoffRetryStrategy;
import org.apache.uniffle.client.retry.RetryFactors;
import org.apache.uniffle.client.retry.RetryStrategy;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;

import static org.apache.uniffle.common.ShuffleDataDistributionType.LOCAL_ORDER;
import static org.junit.jupiter.api.Assertions.fail;

public class ShuffleServerGrpcClientTest {

  private RetryStrategy createRetryStrategy() {
    Function<RetryFactors, Boolean> retryFunction = (factors) -> "UNAVAILABLE".equals(factors.getRpcStatus());
    return new DefaultBackoffRetryStrategy(retryFunction, 2, 1000, 1000);
  }

  @Test
  public void retryWhenServerDown() {
    ClientInterceptor[] clientInterceptors = new ClientInterceptor[]{
        new RetryInterceptor(createRetryStrategy())
    };

    ShuffleServerGrpcClient grpcClient = new ShuffleServerGrpcClient("127.0.0.1", 19999, clientInterceptors);

    AtomicReference<Boolean> finished = new AtomicReference<>(false);
    new Thread(() -> {
      try {
        RssRegisterShuffleRequest rrsr = new RssRegisterShuffleRequest("testAppId", 0,
            Lists.newArrayList(new PartitionRange(1, 1)), new RemoteStorageInfo(""), "", LOCAL_ORDER);
        grpcClient.registerShuffle(rrsr);
        fail();
      } catch (Exception e) {
        finished.set(true);
      }
    }).start();
    Awaitility.await().timeout(Duration.ofSeconds(3)).until(() -> finished.get());
  }
}
