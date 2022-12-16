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
import org.apache.uniffle.server.ShuffleDataFlushEvent;
import org.apache.uniffle.server.ShuffleDataReadEvent;
import org.apache.uniffle.server.event.AppPurgeEvent;
import org.apache.uniffle.server.event.PurgeEvent;
import org.apache.uniffle.server.event.ShufflePurgeEvent;
import org.apache.uniffle.storage.common.LocalStorage;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.util.ShuffleStorageUtils;

public class ChainableLocalStorageSelector extends AbstractCacheableStorageSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChainableLocalStorageSelector.class);

  private final List<LocalStorage> localStorages;
  private final Map<String, LocalStorageView> storageOfPartitions;

  public ChainableLocalStorageSelector(List<LocalStorage> localStorages) {
    this.localStorages = localStorages;
    this.storageOfPartitions = Maps.newConcurrentMap();
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
          LOGGER.error("LocalStorage: " + lastStorage.getBasePath() + " is corrupted.");
        }
      } else {
        if (lastStorage.canWrite()) {
          return lastStorage;
        }
      }
    }

    List<LocalStorage> candidates = localStorages
        .stream()
        .filter(x -> x.canWrite() && !x.isCorrupted())
        .collect(Collectors.toList());
    LocalStorage localStorage = candidates.get(
        ShuffleStorageUtils.getStorageIndex(
            candidates.size(),
            appId,
            shuffleId,
            partitionId
        )
    );

    LocalStorage finalLastStorage = lastStorage;
    storageOfPartitions.compute(
        getKey(event),
        (key, storageView) -> {
          if (storageView == null) {
            return new LocalStorageView(localStorage);
          }
          if (finalLastStorage != null && finalLastStorage.isCorrupted()
              && !finalLastStorage.containsWriteHandler(appId, shuffleId, partitionId)) {
            storageView.removeTail();
          }
          storageView.add(localStorage);
          return storageView;
        }
    );
    event.setUnderStorage(localStorage);
    return localStorage;
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
    return storageOfPartitions.get(getKey(event)).get(event.getStorageIndex());
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