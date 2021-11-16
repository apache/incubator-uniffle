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

package com.tencent.rss.client.api;

import com.tencent.rss.client.request.RssAppHeartBeatRequest;
import com.tencent.rss.client.request.RssFinishShuffleRequest;
import com.tencent.rss.client.request.RssGetShuffleDataRequest;
import com.tencent.rss.client.request.RssGetShuffleIndexRequest;
import com.tencent.rss.client.request.RssGetShuffleResultRequest;
import com.tencent.rss.client.request.RssRegisterShuffleRequest;
import com.tencent.rss.client.request.RssReportShuffleResultRequest;
import com.tencent.rss.client.request.RssSendCommitRequest;
import com.tencent.rss.client.request.RssSendShuffleDataRequest;
import com.tencent.rss.client.response.RssAppHeartBeatResponse;
import com.tencent.rss.client.response.RssFinishShuffleResponse;
import com.tencent.rss.client.response.RssGetShuffleDataResponse;
import com.tencent.rss.client.response.RssGetShuffleIndexResponse;
import com.tencent.rss.client.response.RssGetShuffleResultResponse;
import com.tencent.rss.client.response.RssRegisterShuffleResponse;
import com.tencent.rss.client.response.RssReportShuffleResultResponse;
import com.tencent.rss.client.response.RssSendCommitResponse;
import com.tencent.rss.client.response.RssSendShuffleDataResponse;

public interface ShuffleServerClient {

  RssRegisterShuffleResponse registerShuffle(RssRegisterShuffleRequest request);

  RssSendShuffleDataResponse sendShuffleData(RssSendShuffleDataRequest request);

  RssSendCommitResponse sendCommit(RssSendCommitRequest request);

  RssAppHeartBeatResponse sendHeartBeat(RssAppHeartBeatRequest request);

  RssFinishShuffleResponse finishShuffle(RssFinishShuffleRequest request);

  RssReportShuffleResultResponse reportShuffleResult(RssReportShuffleResultRequest request);

  RssGetShuffleResultResponse getShuffleResult(RssGetShuffleResultRequest request);

  RssGetShuffleIndexResponse getShuffleIndex(RssGetShuffleIndexRequest request);

  RssGetShuffleDataResponse getShuffleData(RssGetShuffleDataRequest request);

  String getDesc();

  void close();

  String getClientInfo();
}
