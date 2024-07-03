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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AutoCloseWrapperTest {

  @Test
  void test1() {
    Supplier<MockClient> cf = () -> new MockClient(false);
    AutoCloseWrapper<MockClient> mockClientAutoCloseWrapper = new AutoCloseWrapper<>(cf);

    MockClient mockClient = mockClientAutoCloseWrapper.get();
    MockClient mockClient2 = mockClientAutoCloseWrapper.get();
    assertTrue(mockClient == mockClient2);
    assertEquals(mockClientAutoCloseWrapper.getRefCount(), 2);
    mockClientAutoCloseWrapper.closeInternal();
    mockClientAutoCloseWrapper.closeInternal();
    assertEquals(mockClientAutoCloseWrapper.getRefCount(), 0);
  }

  @Test
  void test2() {
    Supplier<MockClient> cf = () -> new MockClient(true);
    AutoCloseWrapper<MockClient> mockClientAutoCloseWrapper = new AutoCloseWrapper<>(cf, 10);
    assertEquals(mockClientAutoCloseWrapper.getRefCount(), 0);
    MockClient mockClient1 = mockClientAutoCloseWrapper.get();
    assertNotNull(mockClient1);
    assertEquals(mockClientAutoCloseWrapper.getRefCount(), 1);
    AutoCloseWrapper.run(
        mockClientAutoCloseWrapper,
        (MockClient mockClient) -> {
          assertEquals(mockClientAutoCloseWrapper.getRefCount(), 2);
          return "t1";
        });
    Uninterruptibles.sleepUninterruptibly(30, TimeUnit.MILLISECONDS);
    assertEquals(mockClientAutoCloseWrapper.getRefCount(), 1);
    mockClientAutoCloseWrapper.closeInternal();
    assertEquals(mockClientAutoCloseWrapper.getRefCount(), 0);
  }

  @Test
  void forceClose() {
    Supplier<MockClient> cf = () -> new MockClient(true);
    AutoCloseWrapper<MockClient> mockClientAutoCloseWrapper = new AutoCloseWrapper<>(cf);
    MockClient mockClient = mockClientAutoCloseWrapper.get();
    MockClient mockClient2 = mockClientAutoCloseWrapper.get();
    assertEquals(mockClientAutoCloseWrapper.getRefCount(), 2);
    try {
      mockClientAutoCloseWrapper.forceClose();
    } catch (IOException e) {
      // ignore
    }
    assertEquals(mockClientAutoCloseWrapper.getRefCount(), 0);
  }

  static class MockClient implements Closeable {
    boolean withException;

    MockClient(boolean withException) {
      this.withException = withException;
    }

    @Override
    public void close() throws IOException {
      if (withException) {
        throw new IOException("test exception!");
      }
    }
  }
}
