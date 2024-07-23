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

package org.apache.uniffle.server.merge;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.ShufflePartitionedData;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.server.ShuffleServer;

public class ShuffleEntity<K, V> {

  final RssConf serverConf;
  final String appId;
  final int shuffleId;
  final Class<K> kClass;
  final Class<V> vClass;
  final Comparator<K> comparator;
  final MergeEventHandler eventHandler;
  final ShuffleServer shuffleServer;
  // partition --> PartitionEntity
  private final Map<Integer, PartitionEntity<K, V>> entities = JavaUtils.newConcurrentMap();
  final int mergedBlockSize;
  final ClassLoader classLoader;

  public ShuffleEntity(
      RssConf rssConf,
      MergeEventHandler eventHandler,
      ShuffleServer shuffleServer,
      String appId,
      int shuffleId,
      Class<K> kClass,
      Class<V> vClass,
      Comparator<K> comparator,
      int mergedBlockSize,
      ClassLoader classLoader) {
    this.serverConf = rssConf;
    this.eventHandler = eventHandler;
    this.shuffleServer = shuffleServer;
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.kClass = kClass;
    this.vClass = vClass;
    this.comparator = comparator;
    this.mergedBlockSize = mergedBlockSize;
    this.classLoader = classLoader;
  }

  public void reportUniqueBlockIds(int partitionId, Roaring64NavigableMap expectedBlockIdMap)
      throws IOException {
    this.entities.putIfAbsent(partitionId, new PartitionEntity<K, V>(this, partitionId));
    this.entities.get(partitionId).reportUniqueBlockIds(expectedBlockIdMap);
  }

  void cleanup() {
    for (PartitionEntity entity : this.entities.values()) {
      entity.cleanup();
    }
    this.entities.clear();
  }

  public void cacheBlock(ShufflePartitionedData spd) throws IOException {
    int partitionId = spd.getPartitionId();
    this.entities.putIfAbsent(partitionId, new PartitionEntity<K, V>(this, partitionId));
    for (ShufflePartitionedBlock block : spd.getBlockList()) {
      this.entities.get(partitionId).cacheBlock(block);
    }
  }

  public ClassLoader getClassLoader() {
    return classLoader;
  }

  @VisibleForTesting
  PartitionEntity getPartitionEntity(int partition) {
    return this.entities.get(partition);
  }
}
