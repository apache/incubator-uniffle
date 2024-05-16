package org.apache.uniffle.server.merge;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.ShufflePartitionedData;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.merger.AbstractSegment;
import org.apache.uniffle.common.merger.MergeState;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.server.ShuffleTaskManager;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class ShuffleEntity<K, V> {

  final RssConf serverConf;
  final String appId;
  final int shuffleId;
  final Class<K> kClass;
  final Class<V> vClass;
  final Comparator<K> comparator;
  final MergeEventHandler eventHandler;
  final ShuffleTaskManager taskManager;
  final List<String> localDirs;
  // partition --> PartitionEntity
  private final Map<Integer, PartitionEntity<K, V>> entities = JavaUtils.newConcurrentMap();
  final int mergedBlockSize;
  final ClassLoader classLoader;

  public ShuffleEntity(RssConf rssConf, MergeEventHandler eventHandler, ShuffleTaskManager taskManager,
                       List<String> localDirs, String appId, int shuffleId, Class<K> kClass, Class<V> vClass,
                       Comparator<K> comparator, int mergedBlockSize, ClassLoader classLoader) {
    this.serverConf = rssConf;
    this.eventHandler = eventHandler;
    this.taskManager = taskManager;
    this.localDirs = localDirs;
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.kClass = kClass;
    this.vClass = vClass;
    this.comparator = comparator;
    this.mergedBlockSize = mergedBlockSize;
    this.classLoader = classLoader;
  }

  public void reportUniqueBlockIds(int partitionId, Roaring64NavigableMap expectedBlockIdMap) throws IOException {
    this.entities.putIfAbsent(partitionId, new PartitionEntity<K, V>(this, partitionId));
    this.entities.get(partitionId).reportUniqueBlockIds(expectedBlockIdMap);
  }

  public void merge(int partition, List<AbstractSegment> segments, int openFiles) throws IOException {
    this.entities.get(partition).merge(segments, openFiles);
  }

  public Pair<MergeState, Long> tryGetBlock(int partitionId, long blockId) {
    return this.entities.get(partitionId).tryGetBlock(blockId);
  }

  void cleanup() {
    this.entities.forEach((i, pe) -> pe.cleanup());
    this.entities.clear();
  }

  public void cacheBlock(ShufflePartitionedData spd) {
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
