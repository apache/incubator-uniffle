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

import com.google.common.collect.Lists;
import io.grpc.ClientInterceptor;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.client.request.RssRegisterShuffleRequest;
import org.apache.uniffle.client.retry.StatefulUpgradeRetryStrategy;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;

import static org.apache.uniffle.common.ShuffleDataDistributionType.LOCAL_ORDER;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ShuffleServerGrpcClientTest {

  @Test
  public void retryWhenServerDown() {
    ClientInterceptor[] clientInterceptors = new ClientInterceptor[]{
        new RetryInterceptor(new StatefulUpgradeRetryStrategy(3, 1000, 1000))
    };

    ShuffleServerGrpcClient grpcClient = new ShuffleServerGrpcClient("127.0.0.1", 19999, clientInterceptors);

    long start = System.currentTimeMillis();
    try {
      RssRegisterShuffleRequest rrsr = new RssRegisterShuffleRequest("testAppId", 0,
          Lists.newArrayList(new PartitionRange(1, 1)), new RemoteStorageInfo(""), "", LOCAL_ORDER);
      grpcClient.registerShuffle(rrsr);
      fail();
    } catch (Exception e) {
      // ignore
    }
    assertTrue(System.currentTimeMillis() - start > 2 * 1000);
  }
}
