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

import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.config.RssConf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** Tests enum type {@link ReconfigurableRegistry}. */
public class ReconfigurableRegistryTest {

  @Test
  public void testUpdate() {
    ReconfigurableBad bad = new ReconfigurableBad();
    ReconfigurableGood good0 = new ReconfigurableGood();
    ReconfigurableGood good1 = new ReconfigurableGood();
    try {
      ReconfigurableRegistry.register(good0);
      ReconfigurableRegistry.register(bad);
      ReconfigurableRegistry.register(good1);
      try {
        ReconfigurableRegistry.update(null, null);
        fail("should not success");
      } catch (IllegalStateException e) {
        assertTrue(e.getCause().getMessage().contains("I am bad guy"));
      }
      assertEquals(1, good0.mInvokeCount);
      assertEquals(1, good1.mInvokeCount);
      try {
        ReconfigurableRegistry.update(null, Collections.singletonMap("key", "value"));
        fail("should not success");
      } catch (IllegalStateException e) {
        assertTrue(e.getCause().getMessage().contains("I am bad guy"));
      }
      assertEquals(2, good0.mInvokeCount);
      assertEquals(2, good1.mInvokeCount);
      // remove bad guy
      ReconfigurableRegistry.unregister(bad);
      ReconfigurableRegistry.update(null, null);
      assertEquals(3, good0.mInvokeCount);
      assertEquals(3, good1.mInvokeCount);
      ReconfigurableRegistry.update(null, Collections.singletonMap("key", "value"));
      assertEquals(4, good0.mInvokeCount);
      assertEquals(4, good1.mInvokeCount);
    } finally {
      ReconfigurableRegistry.unregister(bad);
      ReconfigurableRegistry.unregister(good0);
      ReconfigurableRegistry.unregister(good1);
    }
  }

  class ReconfigurableBad implements ReconfigurableRegistry.ReconfigureListener {
    @Override
    public void update(RssConf conf, Map<String, Object> changedProperties) {
      throw new RuntimeException("I am bad guy");
    }
  }

  class ReconfigurableGood implements ReconfigurableRegistry.ReconfigureListener {
    int mInvokeCount = 0;

    @Override
    public void update(RssConf rssConf, Map<String, Object> changedProperties) {
      mInvokeCount++;
    }
  }
}
