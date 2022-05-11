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

package com.tencent.rss.client.impl;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.tencent.rss.client.api.ShuffleServerClient;
import com.tencent.rss.client.response.ResponseStatusCode;
import com.tencent.rss.client.response.RssSendShuffleDataResponse;
import com.tencent.rss.client.response.SendShuffleDataResult;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;

import java.nio.ByteBuffer;
import java.util.List;
import org.junit.Test;

public class ShuffleWriteClientImplTest {

  @Test
  public void testSendData() {
    ShuffleWriteClientImpl shuffleWriteClient =
        new ShuffleWriteClientImpl("GRPC", 3, 2000, 4, 1, 1, 1, true);
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = spy(shuffleWriteClient);
    doReturn(mockShuffleServerClient).when(spyClient).getShuffleServerClient(any());
    when(mockShuffleServerClient.sendShuffleData(any())).thenReturn(
        new RssSendShuffleDataResponse(ResponseStatusCode.NO_BUFFER));

    List<ShuffleServerInfo> shuffleServerInfoList =
        Lists.newArrayList(new ShuffleServerInfo("id", "host", 0));
    List<ShuffleBlockInfo> shuffleBlockInfoList = Lists.newArrayList(new ShuffleBlockInfo(
        0, 0, 10, 10, 10, new byte[]{1}, shuffleServerInfoList, 10, 100, 0));
    SendShuffleDataResult result = spyClient.sendShuffleData("appId", shuffleBlockInfoList);

    assertTrue(result.getFailedBlockIds().contains(10L));
  }
}
