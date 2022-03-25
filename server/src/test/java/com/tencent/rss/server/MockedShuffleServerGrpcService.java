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

package com.tencent.rss.server;

import com.google.common.util.concurrent.Uninterruptibles;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.proto.RssProtos;

import java.util.concurrent.TimeUnit;

public class MockedShuffleServerGrpcService extends ShuffleServerGrpcService {

  private static final Logger LOG = LoggerFactory.getLogger(MockedShuffleServerGrpcService.class);

  private long mockedTimeout = -1L;

  public void enableMockedTimeout(long timeout) {
    mockedTimeout = timeout;
  }

  public void disableMockedTimeout() {
    mockedTimeout = -1;
  }

  public MockedShuffleServerGrpcService(ShuffleServer shuffleServer) {
    super(shuffleServer);
  }

  @Override
  public void sendShuffleData(RssProtos.SendShuffleDataRequest request,
      StreamObserver<RssProtos.SendShuffleDataResponse> responseObserver) {
    if (mockedTimeout > 0) {
      LOG.info("Add a mocked timeout on sendShuffleData");
        Uninterruptibles.sleepUninterruptibly(mockedTimeout, TimeUnit.MILLISECONDS);
    }
    super.sendShuffleData(request, responseObserver);
  }

  @Override
  public void reportShuffleResult(RssProtos.ReportShuffleResultRequest request,
                              StreamObserver<RssProtos.ReportShuffleResultResponse> responseObserver) {
    if (mockedTimeout > 0) {
      LOG.info("Add a mocked timeout on reportShuffleResult");
      Uninterruptibles.sleepUninterruptibly(mockedTimeout, TimeUnit.MILLISECONDS);
    }
    super.reportShuffleResult(request, responseObserver);
  }

  @Override
  public void getShuffleResult(RssProtos.GetShuffleResultRequest request,
                               StreamObserver<RssProtos.GetShuffleResultResponse> responseObserver) {
    if (mockedTimeout > 0) {
      LOG.info("Add a mocked timeout on getShuffleResult");
      Uninterruptibles.sleepUninterruptibly(mockedTimeout, TimeUnit.MILLISECONDS);
    }
    super.getShuffleResult(request, responseObserver);
  }
}
