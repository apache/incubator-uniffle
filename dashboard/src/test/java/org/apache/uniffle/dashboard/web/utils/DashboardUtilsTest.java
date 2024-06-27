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

package org.apache.uniffle.dashboard.web.utils;

import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DashboardUtilsTest {
  @Test
  public void testConvertToMap() {
    String coordinatorStr =
        "http://coordinator.hostname00:19998/,http://coordinator.hostname01:19998/,http://coordinator.hostname02:19998/,http://coordinator.hostname03:19998/";
    Map<String, String> coordinatorAddressMap =
        DashboardUtils.convertAddressesStrToMap(coordinatorStr);
    assertEquals(coordinatorAddressMap.size(), 4);
  }
}
