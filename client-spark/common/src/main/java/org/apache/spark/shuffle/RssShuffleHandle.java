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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  private static final ShuffleHandleInfo EMPTY_HANDLE_INFO = new ShuffleHandleInfo(-1, Collections.EMPTY_MAP,
      RemoteStorageInfo.EMPTY_REMOTE_STORAGE);
  private static volatile WeakReference<ShuffleHandleInfo> _currentHandleInfo = new WeakReference<>(EMPTY_HANDLE_INFO);
  private static ThreadLocal<WeakReference<ShuffleHandleInfo>> _localHandleInfo = ThreadLocal.withInitial(
      () -> new WeakReference<>(EMPTY_HANDLE_INFO));

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
   * Tried to get cached {@link ShuffleHandleInfo} from local thread first and then memory if not existing in local.
   * If not cached, one of competing threads gets chance to deserialize it and caches it for other threads.
   *
   * @return
   */
  private ShuffleHandleInfo getCurrentHandleInfo() {
    // local first
    ShuffleHandleInfo info = _localHandleInfo.get().get();
    if (info != null && (shuffleId() == info.getShuffleId())) { // check if it's GCed and within same shuffle
      return info;
    }
    // check memory without lock
    info = _currentHandleInfo.get();
    if (info != null && (shuffleId() == info.getShuffleId())) {
      _localHandleInfo.set(new WeakReference<>(info));
      return info;
    }
    // last restore to lock
    synchronized (EMPTY_HANDLE_INFO) {
      // check memory again in case the handle was deser and set by other thread
      info = _currentHandleInfo.get();
      if (info != null && (shuffleId() == info.getShuffleId())) {
        _localHandleInfo.set(new WeakReference<>(info));
        return info;
      }
      // deserialize and set to both local and memory
      info = SparkEnv.get().closureSerializer().newInstance()
          .deserialize(ByteBuffer.wrap(handlerInfoBytesBd.value()),
              RssSparkShuffleUtils.SHUFFLE_HANDLER_INFO_CLASS_TAG);
      _localHandleInfo.set(new WeakReference<>(info));
      _currentHandleInfo = new WeakReference<>(info);
      return info;
    }
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

}
