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

package com.tencent.rss.client.factory;

import com.tencent.rss.client.api.ShuffleReadClient;
import com.tencent.rss.client.api.ShuffleWriteClient;
import com.tencent.rss.client.impl.ShuffleReadClientImpl;
import com.tencent.rss.client.impl.ShuffleWriteClientImpl;
import com.tencent.rss.client.request.CreateShuffleReadClientRequest;

public class ShuffleClientFactory {

  private static final ShuffleClientFactory INSTANCE = new ShuffleClientFactory();

  private ShuffleClientFactory() {
  }

  public static ShuffleClientFactory getInstance() {
    return INSTANCE;
  }

  public ShuffleWriteClient createShuffleWriteClient(
      String clientType, int retryMax, long retryIntervalMax, int heartBeatThreadNum,
      int replica, int replicaWrite, int replicaRead, boolean replicaSkipEnabled, int dataTransferPoolSize) {
    return new ShuffleWriteClientImpl(clientType, retryMax, retryIntervalMax, heartBeatThreadNum,
      replica, replicaWrite, replicaRead, replicaSkipEnabled, dataTransferPoolSize);
  }

  public ShuffleReadClient createShuffleReadClient(CreateShuffleReadClientRequest request) {
    return new ShuffleReadClientImpl(request.getStorageType(), request.getAppId(), request.getShuffleId(),
        request.getPartitionId(), request.getIndexReadLimit(), request.getPartitionNumPerRange(),
        request.getPartitionNum(), request.getReadBufferSize(), request.getBasePath(),
        request.getBlockIdBitmap(), request.getTaskIdBitmap(), request.getShuffleServerInfoList(),
        request.getHadoopConf(), request.getIdHelper());
  }
}
