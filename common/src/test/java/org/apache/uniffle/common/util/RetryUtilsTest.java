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

package org.apache.uniffle.common.util;

import com.google.common.collect.Sets;
import org.apache.uniffle.common.exception.RssException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RetryUtilsTest {
  @Test
  public void testRetry() {
    AtomicInteger retryTimes = new AtomicInteger();
    AtomicInteger callbackTime = new AtomicInteger();
    int maxRetryTime = 3;
    try {
      RetryUtils.retry(() -> {
        retryTimes.incrementAndGet();
        throw new RssException("");
      }, () -> {
        callbackTime.incrementAndGet();
      }, 10, maxRetryTime, Sets.newHashSet(RssException.class));
    } catch (Throwable throwable) {
    }
    assertEquals(retryTimes.get(), maxRetryTime);
    assertEquals(callbackTime.get(), maxRetryTime - 1);

    retryTimes.set(0);
    try {
      RetryUtils.retry(() -> {
        retryTimes.incrementAndGet();
        throw new Exception("");
      }, 10, maxRetryTime);
    } catch (Throwable throwable) {
    }
    assertEquals(retryTimes.get(), maxRetryTime);

    retryTimes.set(0);
    try {
      int ret = RetryUtils.retry(() -> {
        retryTimes.incrementAndGet();
        return 1;
      }, 10, maxRetryTime);
      assertEquals(ret, 1);
    } catch (Throwable throwable) {
    }
    assertEquals(retryTimes.get(), 1);
  }
}
