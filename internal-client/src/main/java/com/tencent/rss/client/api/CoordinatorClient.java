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

package com.tencent.rss.client.api;

import com.tencent.rss.client.request.RssAccessClusterRequest;
import com.tencent.rss.client.request.RssAppHeartBeatRequest;
import com.tencent.rss.client.request.RssFetchClientConfRequest;
import com.tencent.rss.client.request.RssFetchRemoteStorageRequest;
import com.tencent.rss.client.request.RssGetShuffleAssignmentsRequest;
import com.tencent.rss.client.request.RssSendHeartBeatRequest;
import com.tencent.rss.client.response.RssAccessClusterResponse;
import com.tencent.rss.client.response.RssAppHeartBeatResponse;
import com.tencent.rss.client.response.RssFetchClientConfResponse;
import com.tencent.rss.client.response.RssFetchRemoteStorageResponse;
import com.tencent.rss.client.response.RssGetShuffleAssignmentsResponse;
import com.tencent.rss.client.response.RssSendHeartBeatResponse;

public interface CoordinatorClient {

  RssAppHeartBeatResponse sendAppHeartBeat(RssAppHeartBeatRequest request);

  RssSendHeartBeatResponse sendHeartBeat(RssSendHeartBeatRequest request);

  RssGetShuffleAssignmentsResponse getShuffleAssignments(RssGetShuffleAssignmentsRequest request);

  RssAccessClusterResponse accessCluster(RssAccessClusterRequest request);

  RssFetchClientConfResponse fetchClientConf(RssFetchClientConfRequest request);

  RssFetchRemoteStorageResponse fetchRemoteStorage(RssFetchRemoteStorageRequest request);

  String getDesc();

  void close();
}
