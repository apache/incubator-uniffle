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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExpiringCloseableSupplierTest {

  @Test
  void testCacheable() {
    Supplier<MockClient> cf = () -> new MockClient(false);
    ExpiringCloseableSupplier<MockClient> mockClientSupplier = new ExpiringCloseableSupplier<>(cf);

    MockClient mockClient = mockClientSupplier.get();
    MockClient mockClient2 = mockClientSupplier.get();
    assertSame(mockClient, mockClient2);
    mockClientSupplier.close();
    mockClientSupplier.close();
  }

  @Test
  void testAutoCloseable() {
    Supplier<MockClient> cf = () -> new MockClient(true);
    ExpiringCloseableSupplier<MockClient> mockClientSupplier =
        new ExpiringCloseableSupplier<>(cf, 10);
    MockClient mockClient1 = mockClientSupplier.get();
    assertNotNull(mockClient1);
    Uninterruptibles.sleepUninterruptibly(30, TimeUnit.MILLISECONDS);
    assertTrue(mockClient1.isClosed());
    MockClient mockClient2 = mockClientSupplier.get();
    assertNotSame(mockClient1, mockClient2);
    mockClientSupplier.close();
  }

  @Test
  void testRenew() {
    Supplier<MockClient> cf = () -> new MockClient(true);
    ExpiringCloseableSupplier<MockClient> mockClientSupplier = new ExpiringCloseableSupplier<>(cf);
    MockClient mockClient = mockClientSupplier.get();
    mockClientSupplier.close();
    MockClient mockClient2 = mockClientSupplier.get();
    assertNotSame(mockClient, mockClient2);
  }

  @Test
  void testReClose() {
    Supplier<MockClient> cf = () -> new MockClient(true);
    ExpiringCloseableSupplier<MockClient> mockClientSupplier = new ExpiringCloseableSupplier<>(cf);
    mockClientSupplier.get();
    mockClientSupplier.close();
    mockClientSupplier.close();
  }

  @Test
  void testDelegateExtendClose() throws IOException {
    Supplier<MockClient> cf = () -> new MockClient(false);
    ExpiringCloseableSupplier<MockClient> mockClientSupplier = new ExpiringCloseableSupplier<>(cf);
    MockClient mockClient = mockClientSupplier.get();
    mockClient.close();
    assertTrue(mockClient.isClosed());

    MockClient mockClient1 = mockClientSupplier.get();
    assertNotSame(mockClient, mockClient1);
    MockClient mockClient2 = mockClientSupplier.get();
    assertSame(mockClient1, mockClient2);
    mockClientSupplier.close();
  }

  static class MockClient implements CloseStateful {
    boolean withException;
    AtomicBoolean closed = new AtomicBoolean(false);

    MockClient(boolean withException) {
      this.withException = withException;
    }

    @Override
    public void close() throws IOException {
      closed.set(true);
      if (withException) {
        throw new IOException("test exception!");
      }
    }

    @Override
    public boolean isClosed() {
      return closed.get();
    }
  }
}
