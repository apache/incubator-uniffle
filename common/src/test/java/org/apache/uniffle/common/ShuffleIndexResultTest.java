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

package org.apache.uniffle.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShuffleIndexResultTest {

  @Test
  public void testEmpty() {
    assertTrue(new ShuffleIndexResult().isEmpty());
    assertTrue(new ShuffleIndexResult((byte[]) null, -1).isEmpty());
  }

  @Test
  public void testRelease() {
    ShuffleIndexResult shuffleIndexResult = new ShuffleIndexResult("test".getBytes(), -1);
    shuffleIndexResult.release();
    // Expect no exception when executing release again
    assertDoesNotThrow(shuffleIndexResult::release);
    assertTrue(shuffleIndexResult.isEmpty());
  }
}
