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

import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Collections;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.RemoteStorageInfo;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RssShuffleHandleTest {

  private Field localField = FieldUtils.getField(RssShuffleHandle.class, "_localHandleInfo",
      true);
  private Field currentField = FieldUtils.getField(RssShuffleHandle.class, "_currentHandleInfo",
      true);

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
    int shuffleId = 1;
    RssShuffleHandle handle = createShuffleHandle("appId1", shuffleId);

    int numThreads = 10;
    ShuffleHandleInfo[] infoFromThr = new ShuffleHandleInfo[numThreads];
    AtomicInteger index = new AtomicInteger(0);
    Runnable task = () -> {
      handle.getRemoteStorage();
      try {
        ThreadLocal<WeakReference<ShuffleHandleInfo>> local = (ThreadLocal<WeakReference<ShuffleHandleInfo>>)
            localField.get(null);
        infoFromThr[index.getAndIncrement()] = local.get().get();
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
    // check if all infos are same
    WeakReference<ShuffleHandleInfo> current = (WeakReference<ShuffleHandleInfo>)currentField.get(null);
    ShuffleHandleInfo currentInfo = current.get();
    ShuffleHandleInfo threadInfo = infoFromThr[0];
    for (int i = 0; i < numThreads; i++) {
      Assertions.assertEquals(threadInfo, infoFromThr[i]);
      Assertions.assertEquals(currentInfo, infoFromThr[i]);
      Assertions.assertEquals(shuffleId, infoFromThr[i].getShuffleId());
    }
  }

  @Test
  public void testMultipleShuffleStages() throws Exception {
    int shuffleId1 = 1;
    int shuffleId2 = 2;
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
      ThreadLocal<WeakReference<ShuffleHandleInfo>> local = null;
      try {
        local = (ThreadLocal<WeakReference<ShuffleHandleInfo>>)
            localField.get(null);
      } catch (Exception e) {
        e.printStackTrace();
      }
      infoFromThr1[index1.getAndIncrement()] = local.get().get();
      // simulate stages
      latch.countDown();
      try {
        latch.await(100, TimeUnit.SECONDS);
      } catch (Exception e) {
        e.printStackTrace();
      }
      handle2.getRemoteStorage();
      infoFromThr2[index2.getAndIncrement()] = local.get().get();
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
    // check if all infos are same
    WeakReference<ShuffleHandleInfo> current = (WeakReference<ShuffleHandleInfo>)currentField.get(null);
    ShuffleHandleInfo currentInfo = current.get();
    ShuffleHandleInfo threadInfo1 = infoFromThr1[0];
    for (int i = 0; i < numThreads; i++) {
      Assertions.assertEquals(threadInfo1, infoFromThr1[i]);
      Assertions.assertNotEquals(currentInfo, infoFromThr1[i]);
      Assertions.assertEquals(shuffleId1, infoFromThr1[i].getShuffleId());
    }
    ShuffleHandleInfo threadInfo2 = infoFromThr2[0];
    for (int i = 0; i < numThreads; i++) {
      Assertions.assertEquals(threadInfo2, infoFromThr2[i]);
      Assertions.assertEquals(currentInfo, infoFromThr2[i]);
      Assertions.assertEquals(shuffleId2, infoFromThr2[i].getShuffleId());
    }
  }
}
