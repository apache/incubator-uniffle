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

package com.tencent.rss.coordinator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Objects;
import org.junit.jupiter.api.Test;

public class CoordinatorConfTest {

  @Test
  public void test() {
    final String filePath = Objects.requireNonNull(
        getClass().getClassLoader().getResource("coordinator.conf")).getFile();
    CoordinatorConf conf = new CoordinatorConf(filePath);

    // test base conf
    assertEquals(9527, conf.getInteger(CoordinatorConf.RPC_SERVER_PORT));
    assertEquals("testRpc", conf.getString(CoordinatorConf.RPC_SERVER_TYPE));
    assertEquals(9526, conf.getInteger(CoordinatorConf.JETTY_HTTP_PORT));

    // test coordinator specific conf
    assertEquals("/a/b/c", conf.getString(CoordinatorConf.COORDINATOR_EXCLUDE_NODES_FILE_PATH));
    assertEquals(37, conf.getInteger(CoordinatorConf.COORDINATOR_SHUFFLE_NODES_MAX));
    assertEquals(123, conf.getLong(CoordinatorConf.COORDINATOR_HEARTBEAT_TIMEOUT));

    // test default conf
    assertEquals("PARTITION_BALANCE", conf.getString(CoordinatorConf.COORDINATOR_ASSIGNMENT_STRATEGY));
    assertEquals(256, conf.getInteger(CoordinatorConf.JETTY_CORE_POOL_SIZE));
    assertEquals(60 * 1000, conf.getLong(CoordinatorConf.COORDINATOR_EXCLUDE_NODES_CHECK_INTERVAL));

  }

}
