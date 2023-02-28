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

package org.apache.spark.shuffle;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.serializer.JavaSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.RemoteStorageInfo;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RssShuffleHandleTest {

  private static Field globalField = FieldUtils.getField(RssShuffleHandle.class, "_globalShuffleIdRefMap",
      true);
  private static Field localField = FieldUtils.getField(RssShuffleHandle.class, "_localHandleInfoCache",
      true);

  public static Map<Integer, RssShuffleHandle.ShuffleIdRef> globalCache;

  public static AtomicInteger ID_SEQ = new AtomicInteger(1);

  static {
    try {
      globalCache =
          (Map<Integer, RssShuffleHandle.ShuffleIdRef>) globalField.get(null);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @BeforeEach
  public void clearCache() {
    globalCache.clear();
  }

  @AfterAll
  public static void clearCacheAfterTest() {
    globalCache.clear();
  }

  private RssShuffleHandle createShuffleHandle(String appId, int shuffleId) {
    Broadcast<byte[]> bytesBd = mock(Broadcast.class);
    SparkEnv env = mock(SparkEnv.class);

    SparkEnv.set(env);
    SparkConf conf = new SparkConf();
    JavaSerializer serializer = new JavaSerializer(conf);
    ShuffleHandleInfo handleInfo = new ShuffleHandleInfo(shuffleId, Collections.EMPTY_MAP,
        RemoteStorageInfo.EMPTY_REMOTE_STORAGE);
    ByteBuffer buffer = serializer.newInstance().serialize(handleInfo,
        RssSparkShuffleUtils.SHUFFLE_HANDLER_INFO_CLASS_TAG);
    byte[] handleInfoBytes = JavaUtils.bufferToArray(buffer);

    when(bytesBd.value()).thenReturn(handleInfoBytes);
    when(env.closureSerializer()).thenReturn(serializer);

    ShuffleDependency dependency = mock(ShuffleDependency.class);
    return new RssShuffleHandle(shuffleId, appId, 2, dependency, bytesBd);
  }

  @Test
  public void testShuffleHandleInfoDeserializeOnce() throws Exception {
    int shuffleId = ID_SEQ.getAndIncrement();
    RssShuffleHandle handle = createShuffleHandle("appId1", shuffleId);

    int numThreads = 10;
    ShuffleHandleInfo[] infoFromThr = new ShuffleHandleInfo[numThreads];
    AtomicInteger index = new AtomicInteger(0);
    Runnable task = () -> {
      handle.getRemoteStorage();
      try {
        ThreadLocal<RssShuffleHandle.HandleInfoLocalCache> local =
            (ThreadLocal<RssShuffleHandle.HandleInfoLocalCache>)
                localField.get(null);
        infoFromThr[index.getAndIncrement()] = local.get().getCurrent().get();
      } catch (Exception e) {
        e.printStackTrace();
      }
    };
    // create and start threads
    Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      threads[i] = new Thread(task);
    }
    for (int i = 0; i < numThreads; i++) {
      threads[i].start();
    }
    for (int i = 0; i < numThreads; i++) {
      threads[i].join();
    }
    // check if all infos are not same
    RssShuffleHandle.ShuffleIdRef idRef = globalCache.get(shuffleId);
    Assertions.assertNotNull(idRef);
    for (int i = 0; i < numThreads; i++) {
      if (i + 1 < numThreads) {
        Assertions.assertNotEquals(infoFromThr[i + 1], infoFromThr[i]);
      }
      Assertions.assertEquals(shuffleId, infoFromThr[i].getShuffleId());
    }
    Assertions.assertEquals(1, globalCache.size());
  }

  @Test
  public void testMultipleSequentialShuffleStages() throws Exception {
    int shuffleId1 = ID_SEQ.getAndIncrement();
    int shuffleId2 = ID_SEQ.getAndIncrement();
    String appId = "appId2";
    RssShuffleHandle handle1 = createShuffleHandle(appId, shuffleId1);
    RssShuffleHandle handle2 = createShuffleHandle(appId, shuffleId2);

    int numThreads = 10;
    CountDownLatch latch = new CountDownLatch(numThreads);
    ShuffleHandleInfo[] infoFromThr1 = new ShuffleHandleInfo[numThreads];
    ShuffleHandleInfo[] infoFromThr2 = new ShuffleHandleInfo[numThreads];
    AtomicInteger index1 = new AtomicInteger(0);
    AtomicInteger index2 = new AtomicInteger(0);
    Runnable task = () -> {
      handle1.getRemoteStorage();
      ThreadLocal<RssShuffleHandle.HandleInfoLocalCache> local = null;
      try {
        local = (ThreadLocal<RssShuffleHandle.HandleInfoLocalCache>)
                localField.get(null);
      } catch (Exception e) {
        e.printStackTrace();
      }
      infoFromThr1[index1.getAndIncrement()] = local.get().getCurrent().get();
      // simulate stages
      latch.countDown();
      try {
        latch.await(100, TimeUnit.SECONDS);
      } catch (Exception e) {
        e.printStackTrace();
      }
      handle2.getRemoteStorage();
      infoFromThr2[index2.getAndIncrement()] = local.get().getCurrent().get();
    };
    // create and start threads
    Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      threads[i] = new Thread(task);
    }
    for (int i = 0; i < numThreads; i++) {
      threads[i].start();
    }
    for (int i = 0; i < numThreads; i++) {
      threads[i].join();
    }
    // check if all infos are not same
    for (int i = 0; i < numThreads; i++) {
      if (i + 1 < numThreads) {
        Assertions.assertNotEquals(infoFromThr1[i + 1], infoFromThr1[i]);
      }
      Assertions.assertEquals(shuffleId1, infoFromThr1[i].getShuffleId());
    }
    for (int i = 0; i < numThreads; i++) {
      if (i + 1 < numThreads) {
        Assertions.assertNotEquals(infoFromThr2[i + 1], infoFromThr2[i]);
      }
      Assertions.assertEquals(shuffleId2, infoFromThr2[i].getShuffleId());
    }
    Assertions.assertEquals(2, globalCache.size());
  }

  @Test
  public void testMultipleConcurrentShuffleStages() throws Exception {
    int shuffleId1 = ID_SEQ.getAndIncrement();
    int shuffleId2 = ID_SEQ.getAndIncrement();
    int shuffleId3 = ID_SEQ.getAndIncrement();
    int shuffleId4 = ID_SEQ.getAndIncrement();
    String appId = "appId3";
    RssShuffleHandle handle1 = createShuffleHandle(appId, shuffleId1);
    RssShuffleHandle handle2 = createShuffleHandle(appId, shuffleId2);
    RssShuffleHandle handle3 = createShuffleHandle(appId, shuffleId3);
    RssShuffleHandle handle4 = createShuffleHandle(appId, shuffleId4);

    int numThreads = 10;
    ShuffleHandleInfo[] infoFromThr1 = new ShuffleHandleInfo[numThreads];
    ShuffleHandleInfo[] infoFromThr2 = new ShuffleHandleInfo[numThreads];
    AtomicInteger index1 = new AtomicInteger(0);
    boolean[] localMapCorrect = new boolean[numThreads];
    for (int i = 0; i < numThreads; i++) {
      localMapCorrect[i] = true;
    }
    Random random = new Random();
    Runnable task = () -> {
      ThreadLocal<RssShuffleHandle.HandleInfoLocalCache> local = null;
      try {
        local = (ThreadLocal<RssShuffleHandle.HandleInfoLocalCache>)
            localField.get(null);
      } catch (Exception e) {
        e.printStackTrace();
      }
      int idx = index1.getAndIncrement();
      if (random.nextBoolean()) {
        handle1.getRemoteStorage();
        infoFromThr1[idx] = local.get().getCurrent().get();
      } else {
        handle2.getRemoteStorage();
        infoFromThr2[idx] = local.get().getCurrent().get();
      }
      handle3.getRemoteStorage();
      handle4.getRemoteStorage();
      handle1.getRemoteStorage();
      handle2.getRemoteStorage();
      localMapCorrect[idx] &= local.get().getHandleInfoMap().size() == 4;
    };
    // create and start threads
    Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      threads[i] = new Thread(task);
    }
    for (int i = 0; i < numThreads; i++) {
      threads[i].start();
    }
    for (int i = 0; i < numThreads; i++) {
      threads[i].join();
    }
    for (int i = 0; i < numThreads; i++) {
      if (infoFromThr1[i] != null) {
        Assertions.assertEquals(shuffleId1, infoFromThr1[i].getShuffleId());
      }
    }

    for (int i = 0; i < numThreads; i++) {
      if (infoFromThr2[i] != null) {
        Assertions.assertEquals(shuffleId2, infoFromThr2[i].getShuffleId());
      }
    }
    for (int i = 0; i < numThreads; i++) {
      Assertions.assertTrue(localMapCorrect[i]);
    }
    Assertions.assertEquals(4, globalCache.size());
    RssShuffleHandle.removeShuffleHandle(shuffleId1);
    RssShuffleHandle.removeShuffleHandle(shuffleId2);
    Assertions.assertEquals(2, globalCache.size());
  }
}
