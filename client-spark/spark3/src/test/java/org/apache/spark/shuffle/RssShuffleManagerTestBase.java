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

package org.apache.spark.shuffle;

import java.util.List;

import com.google.common.collect.Lists;
import org.apache.spark.shuffle.sort.SortShuffleManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import org.apache.uniffle.client.api.CoordinatorClient;
import org.apache.uniffle.client.response.RssAccessClusterResponse;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.Constants;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class RssShuffleManagerTestBase {
  protected static MockedStatic<RssSparkShuffleUtils> mockedStaticRssShuffleUtils;

  @BeforeAll
  public static void setUp() {
    mockedStaticRssShuffleUtils =
        mockStatic(RssSparkShuffleUtils.class, Mockito.CALLS_REAL_METHODS);
  }

  @AfterAll
  public static void tearDown() {
    mockedStaticRssShuffleUtils.close();
  }

  protected ShuffleManager createEssShuffleManager() {
    SortShuffleManager mockedShuffleManager = mock(SortShuffleManager.class);
    ShuffleHandle mockedShuffleHandle = mock(ShuffleHandle.class);
    when(mockedShuffleManager.registerShuffle(anyInt(), any())).thenReturn(mockedShuffleHandle);
    return mockedShuffleManager;
  }

  protected ShuffleManager createRssShuffleManager() {
    RssShuffleManager mockedShuffleManager = mock(RssShuffleManager.class);
    ShuffleHandle mockedShuffleHandle = mock(ShuffleHandle.class);
    when(mockedShuffleManager.registerShuffle(anyInt(), any())).thenReturn(mockedShuffleHandle);
    return mockedShuffleManager;
  }

  protected CoordinatorClient createCoordinatorClient(StatusCode status) {
    CoordinatorClient mockedCoordinatorClient = mock(CoordinatorClient.class);
    when(mockedCoordinatorClient.accessCluster(any()))
        .thenReturn(new RssAccessClusterResponse(status, ""));
    return mockedCoordinatorClient;
  }

  void setupMockedRssShuffleUtils(StatusCode status) {
    CoordinatorClient mockCoordinatorClient = createCoordinatorClient(status);
    List<CoordinatorClient> coordinatorClients = Lists.newArrayList();
    coordinatorClients.add(mockCoordinatorClient);
    mockedStaticRssShuffleUtils
        .when(() -> RssSparkShuffleUtils.createCoordinatorClients(any()))
        .thenReturn(coordinatorClients);
  }

  void setupMockedShuffleManager() {
    ShuffleManager essManager = createEssShuffleManager();
    ShuffleManager rssManager = createRssShuffleManager();
    mockedStaticRssShuffleUtils
        .when(
            () ->
                RssSparkShuffleUtils.loadShuffleManager(
                    eq(Constants.SORT_SHUFFLE_MANAGER_NAME), any(), anyBoolean()))
        .thenReturn(essManager);
  }
}
