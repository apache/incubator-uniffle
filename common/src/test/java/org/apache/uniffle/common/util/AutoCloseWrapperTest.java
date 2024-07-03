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
import java.util.Random;
import java.util.function.Supplier;

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
    closeWrapper(mockClientAutoCloseWrapper);
    closeWrapper(mockClientAutoCloseWrapper);
    assertEquals(mockClientAutoCloseWrapper.getRefCount(), 0);
  }

  @Test
  void test2() {
    Supplier<MockClient> cf = () -> new MockClient(true);
    AutoCloseWrapper<MockClient> mockClientAutoCloseWrapper = new AutoCloseWrapper<>(cf);
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
    assertEquals(mockClientAutoCloseWrapper.getRefCount(), 1);
    closeWrapper(mockClientAutoCloseWrapper);
    assertEquals(mockClientAutoCloseWrapper.getRefCount(), 0);
  }

  @Test
  void test3() throws InterruptedException {
    Supplier<MockClient> cf = () -> new MockClient(false);
    AutoCloseWrapper<MockClient> mockClientAutoCloseWrapper = new AutoCloseWrapper<>(cf);
    for (int i = 0; i < 1000; i++) {
      new Thread(()->{
        AutoCloseWrapper.run(
                mockClientAutoCloseWrapper,
                (MockClient mockClient) -> {
                  mockClient.doSomething();
                  return "t1";
                });
      }).start();
    }
    Thread.sleep(15000);
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
    MockClient mockClient3 = mockClientAutoCloseWrapper.get();
    assertNotNull(mockClient3);
    assertTrue(mockClient3 != mockClient);
  }

  static class MockClient implements Closeable {
    boolean withException;

    volatile boolean isClosed;

    public MockClient(boolean withException) {
      this.withException = withException;
      this.isClosed = false;
    }

    @Override
    public void close() throws IOException {
      new Thread(()->{
        try {
          Thread.sleep(2000);
          isClosed = true;
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }).start();
      if (withException) {
        throw new IOException("test exception!");
      }
    }

    public void doSomething() {
      try {
        Thread.sleep(new Random().nextInt(100));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      if(isClosed) {
        throw new RuntimeException("client is closed.");
      }
    }
  }

  private static void closeWrapper(AutoCloseWrapper<MockClient> mockClientAutoCloseWrapper) {
    try {
      mockClientAutoCloseWrapper.close();
    } catch (IOException e) {
      // ignore
    }
  }
}
