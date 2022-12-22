package org.apache.uniffle.server.storage.local;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.UnionKey;
import org.apache.uniffle.common.exception.FileNotFoundException;
import org.apache.uniffle.server.ShuffleDataFlushEvent;
import org.apache.uniffle.server.ShuffleDataReadEvent;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.event.AppPurgeEvent;
import org.apache.uniffle.server.event.PurgeEvent;
import org.apache.uniffle.server.event.ShufflePurgeEvent;
import org.apache.uniffle.storage.common.LocalStorage;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.util.ShuffleStorageUtils;

import static org.apache.uniffle.server.ShuffleServerConf.RSS_LOCAL_STORAGE_MULTIPLE_DISK_SELECTION_ENABLE;

public class ChainableLocalStorageSelector extends AbstractCacheableStorageSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChainableLocalStorageSelector.class);

  private final List<LocalStorage> localStorages;
  private final Map<String, LocalStorageView> storageOfPartitions;
  private final boolean multipleDiskSelectionEnable;

  public ChainableLocalStorageSelector(ShuffleServerConf shuffleServerConf, List<LocalStorage> localStorages) {
    this.localStorages = localStorages;
    this.storageOfPartitions = Maps.newConcurrentMap();
    this.multipleDiskSelectionEnable = shuffleServerConf.get(RSS_LOCAL_STORAGE_MULTIPLE_DISK_SELECTION_ENABLE);
  }

  @Override
  public Storage selectForWriter(ShuffleDataFlushEvent event) {
    String appId = event.getAppId();
    int shuffleId = event.getShuffleId();
    int partitionId = event.getStartPartition();

    LocalStorageView view = storageOfPartitions.get(getKey(event));
    LocalStorage lastStorage = null;
    if (view != null) {
      lastStorage = view.getLatest();
      if (lastStorage.isCorrupted()) {
        if (lastStorage.containsWriteHandler(appId, shuffleId, partitionId)) {
          LOGGER.error("LocalStorage: {} is corrupted. Switching another storage for event: {}, "
                  + "some data will be lost", lastStorage.getBasePath(), event);
        }
      } else {
        if (!multipleDiskSelectionEnable || lastStorage.canWrite()) {
          return lastStorage;
        }
      }
    }

    // todo: support pluggable selection policy, hash-based or free-space based
    List<LocalStorage> candidates = localStorages
        .stream()
        .filter(x -> x.canWrite() && !x.isCorrupted())
        .collect(Collectors.toList());
    if (candidates.isEmpty()) {
      throw new RuntimeException("No available local storages.");
    }
    final LocalStorage selected = candidates.get(
        ShuffleStorageUtils.getStorageIndex(
            candidates.size(),
            appId,
            shuffleId,
            partitionId
        )
    );

    final LocalStorage previousStorage = lastStorage;
    storageOfPartitions.compute(
        getKey(event),
        (key, storageView) -> {
          if (storageView == null) {
            return new LocalStorageView(selected);
          }
          // If the storage is corrupted, it should be removed from the stoarge view.
          if (previousStorage != null && previousStorage.isCorrupted()) {
            LocalStorage currentTailStorage = storageView.getLatest();
            if (previousStorage == currentTailStorage) {
              storageView.removeTail();
            }
          }
          storageView.add(selected);
          return storageView;
        }
    );
    event.setUnderStorage(selected);
    return selected;
  }

  private String getKey(ShuffleDataFlushEvent event) {
    return UnionKey.buildKey(
        event.getAppId(),
        event.getShuffleId(),
        event.getStartPartition()
    );
  }

  @Override
  public Storage getForReader(ShuffleDataReadEvent event) {
    try {
      return storageOfPartitions.get(getKey(event)).get(event.getStorageIndex());
    } catch (IndexOutOfBoundsException exception) {
      throw new FileNotFoundException("No such local storage for event: " + event);
    }
  }

  @Override
  public void removeCache(PurgeEvent event) {
    Function<String, Boolean> deleteConditionFunc = null;
    if (event instanceof AppPurgeEvent) {
      deleteConditionFunc = partitionUnionKey -> UnionKey.startsWith(partitionUnionKey, event.getAppId());
    } else if (event instanceof ShufflePurgeEvent) {
      deleteConditionFunc =
          partitionUnionKey -> UnionKey.startsWith(
              partitionUnionKey,
              event.getAppId(),
              event.getShuffleIds()
          );
    }
    deleteElement(
        storageOfPartitions,
        deleteConditionFunc
    );
  }

  private <K, V> void deleteElement(Map<K, V> map, Function<K, Boolean> deleteConditionFunc) {
    Iterator<Map.Entry<K, V>> iterator = map.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<K, V> entry = iterator.next();
      if (deleteConditionFunc.apply(entry.getKey())) {
        iterator.remove();
      }
    }
  }

  private String getKey(ShuffleDataReadEvent event) {
    return UnionKey.buildKey(
        event.getAppId(),
        event.getShuffleId(),
        event.getStartPartition()
    );
  }
}