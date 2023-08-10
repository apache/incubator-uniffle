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

package org.apache.uniffle.server;

import java.util.concurrent.atomic.AtomicInteger;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.proto.RssProtos;

public class MockedShuffleServerWriteGrpcService extends ShuffleServerGrpcService {
  private static final Logger LOG =
      LoggerFactory.getLogger(MockedShuffleServerWriteGrpcService.class);
  /** Abnormal node flag */
  private boolean taskExcptionFlag = true;

  private AtomicInteger failedWriteRequest = new AtomicInteger(0);

  public MockedShuffleServerWriteGrpcService(ShuffleServer shuffleServer) {
    super(shuffleServer);
    LOG.info("create MockedShuffleServerWriteGrpcService");
  }

  @Override
  public void sendShuffleData(
      RssProtos.SendShuffleDataRequest req,
      StreamObserver<RssProtos.SendShuffleDataResponse> responseObserver) {
    if (taskExcptionFlag) {
      failedWriteRequest.incrementAndGet();
      throw new RuntimeException(
          "This write request is failed as mocked failure,failed times: "
              + failedWriteRequest.get());
    }
    super.sendShuffleData(req, responseObserver);
  }
}
