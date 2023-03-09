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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkEnv;
import org.apache.spark.broadcast.Broadcast;

import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleServerInfo;

public class RssShuffleHandle<K, V, C> extends ShuffleHandle {

  private String appId;
  private int numMaps;
  private ShuffleDependency<K, V, C> dependency;
  private Broadcast<byte[]> handlerInfoBytesBd;

  // shuffle ID to ShuffleIdRef
  // ShuffleIdRef acts as strong reference to prevent cached ShuffleHandleInfo being GCed during shuffle
  // ShuffleIdRef will be removed when unregisterShuffle()
  private static Map<Integer, ShuffleIdRef> globalShuffleIdRefMap = new ConcurrentHashMap<>();
  // each shuffle has unique ID even for multiple concurrent running shuffles and jobs per application
  private static ThreadLocal<HandleInfoLocalCache> localHandleInfoCache =
      ThreadLocal.withInitial(() -> new HandleInfoLocalCache());

  public RssShuffleHandle(
      int shuffleId,
      String appId,
      int numMaps,
      ShuffleDependency<K, V, C> dependency,
      Broadcast<byte[]> handlerInfoBytesBd) {
    super(shuffleId);
    this.appId = appId;
    this.numMaps = numMaps;
    this.dependency = dependency;
    this.handlerInfoBytesBd = handlerInfoBytesBd;
  }

  public String getAppId() {
    return appId;
  }

  public int getNumMaps() {
    return numMaps;
  }

  public ShuffleDependency<K, V, C> getDependency() {
    return dependency;
  }

  public int getShuffleId() {
    return shuffleId();
  }

  /**
   * Tried to get {@link ShuffleHandleInfo} from local cache first.
   * If not cached, deserialize and cache it in local thread.
   *
   * @return
   */
  private ShuffleHandleInfo getCurrentHandleInfo() {
    // most probably, current ShuffleHandleInfo is desired one
    HandleInfoLocalCache localCache = localHandleInfoCache.get();
    ShuffleHandleInfo info = localCache.current.get();
    int shuffleId = shuffleId();
    if (info != null && shuffleId == info.getShuffleId()) {
      return info;
    }
    // get ShuffleIdRef from global map. It's volatile read most probably
    ShuffleIdRef idRef = globalShuffleIdRefMap.get(shuffleId);
    if (idRef != null && (info = localCache.handleInfoMap.get(idRef)) != null) {
      localCache.current = new WeakReference<>(info);
      return info;
    }
    // threads share same ShuffleIdRef per shuffle
    idRef = globalShuffleIdRefMap.computeIfAbsent(shuffleId, id -> new ShuffleIdRef(id));
    // each thread deserializes one ShuffleHandleInfo per shuffle id
    // Other threads will block here anyway if they all share one ShuffleHandleInfo per shuffle ID
    info = SparkEnv.get().closureSerializer().newInstance()
        .deserialize(ByteBuffer.wrap(handlerInfoBytesBd.value()),
            RssSparkShuffleUtils.SHUFFLE_HANDLER_INFO_CLASS_TAG);
    localCache.current = new WeakReference<>(info);
    localCache.handleInfoMap.put(idRef, info);
    return info;
  }

  public static void removeShuffleHandle(int shuffleId) {
    globalShuffleIdRefMap.remove(shuffleId);
  }

  public RemoteStorageInfo getRemoteStorage() {
    return getCurrentHandleInfo().getRemoteStorage();
  }

  public Map<Integer, List<ShuffleServerInfo>> getPartitionToServers() {
    return getCurrentHandleInfo().getPartitionToServers();
  }

  public Set<ShuffleServerInfo> getShuffleServersForData() {
    return getCurrentHandleInfo().getShuffleServersForData();
  }


  protected static class HandleInfoLocalCache {
    // GCed when no strong reference to ShuffleHandleInfo
    private WeakReference<ShuffleHandleInfo> current = new WeakReference<>(ShuffleHandleInfo.EMPTY_HANDLE_INFO);
    // GCed when no strong reference to ShuffleIdRef
    private WeakHashMap<ShuffleIdRef, ShuffleHandleInfo> handleInfoMap = new WeakHashMap<>();

    public WeakReference<ShuffleHandleInfo> getCurrent() {
      return current;
    }

    public WeakHashMap<ShuffleIdRef, ShuffleHandleInfo> getHandleInfoMap() {
      return handleInfoMap;
    }
  }

  protected static class ShuffleIdRef {
    private int shuffleId;

    public ShuffleIdRef(int shuffleId) {
      this.shuffleId = shuffleId;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof ShuffleIdRef) {
        return shuffleId == ((ShuffleIdRef)o).shuffleId;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Integer.valueOf(shuffleId).hashCode();
    }
  }

}
