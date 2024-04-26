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

package org.apache.uniffle.shuffle.manager;

import java.util.Arrays;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.rpc.ServerType;

public class ShuffleManagerServerFactoryTest {
  private static Stream<Arguments> shuffleManagerServerTypeProvider() {
    return Arrays.stream(ServerType.values()).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("shuffleManagerServerTypeProvider")
  public void testShuffleManagerServerType(ServerType serverType) {
    // add code to generate tests that check the server type
    RssBaseConf conf = new RssBaseConf();
    conf.set(RssBaseConf.RPC_SERVER_TYPE, serverType);
    ShuffleManagerServerFactory factory = new ShuffleManagerServerFactory(null, conf);
    // this should execute normally;
    factory.getServer();
  }
}
