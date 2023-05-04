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

package org.apache.uniffle.server.storage.local;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import org.apache.uniffle.server.ShuffleDataFlushEvent;
import org.apache.uniffle.storage.common.LocalStorage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AvailableSpaceStorageChoosingPolicyTest {

  @Test
  public void test() {
    // case1
    LocalStorage s1 = mock(LocalStorage.class);
    when(s1.canWrite()).thenReturn(true);
    when(s1.isCorrupted()).thenReturn(false);
    when(s1.getDiskSize()).thenReturn(50L);
    when(s1.getCapacity()).thenReturn(200L);

    LocalStorage s2 = mock(LocalStorage.class);
    when(s2.canWrite()).thenReturn(true);
    when(s2.isCorrupted()).thenReturn(false);
    when(s2.getDiskSize()).thenReturn(10L);
    when(s2.getCapacity()).thenReturn(200L);

    ShuffleDataFlushEvent event = new ShuffleDataFlushEvent(
        1,
        "1",
        1,
        1,
        1,
        1,
        Collections.emptyList(),
        null,
        null
    );
    AvailableSpaceStorageChoosingPolicy storageChooser = new AvailableSpaceStorageChoosingPolicy();
    assertEquals(s2, storageChooser.choose(event, s1, s2));

    // case2
    when(s2.canWrite()).thenReturn(false);
    assertEquals(s1, storageChooser.choose(event, s1, s2));

    // case3
    when(s1.isCorrupted()).thenReturn(true);
    assertNull(storageChooser.choose(event, s1, s2));
  }
}
