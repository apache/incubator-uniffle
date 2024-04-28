package org.apache.uniffle.server.merge;

import static org.apache.uniffle.common.util.RssUtils.getConfiguredLocalDirs;
import static org.apache.uniffle.server.ShuffleServerConf.SERVER_MERGE_GET_SEGMENT_V2;
import static org.apache.uniffle.server.ShuffleServerConf.SERVER_MERGE_MAX_OPEN_FILE;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.ShufflePartitionedData;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.merger.AbstractSegment;
import org.apache.uniffle.common.merger.MergeState;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.ShuffleServerMetrics;
import org.apache.uniffle.server.ShuffleTaskManager;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleMergeManager {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleMergeManager.class);
  public static final String MERGE_APP_SUFFIX = "@RemoteMerge";

  private ShuffleServerConf serverConf;
  private ShuffleTaskManager taskManager;
  // appId -> shuffleid -> ShuffleEntity
  private final Map<String, Map<Integer, ShuffleEntity>> entities = JavaUtils.newConcurrentMap();
  private final MergeEventHandler eventHandler;
  private final List<String> localDirs;
  public static Semaphore openFileSemaphore;

  // If comparator is not set, will use hashCode to compare. It is used for shuffle that does not require
  // sort but require combine.
  private Comparator defaultComparator = new Comparator() {
    @Override
    public int compare(Object o1, Object o2) {
      int h1 = (o1 == null) ? 0 : o1.hashCode();
      int h2 = (o2 == null) ? 0 : o2.hashCode();
      return h1 < h2 ? -1 : h1 == h2 ? 0 : 1;
    }
  };

  private boolean getSegmentV2 = true;

  public ShuffleMergeManager(ShuffleServerConf serverConf, ShuffleTaskManager taskManager) {
    this.serverConf = serverConf;
    this.taskManager = taskManager;
    this.eventHandler = new DefaultMergeEventHandler(this.serverConf, this::processEvent);
    this.localDirs = getConfiguredLocalDirs(this.serverConf);
    this.openFileSemaphore = new Semaphore(serverConf.get(SERVER_MERGE_MAX_OPEN_FILE));
    ShuffleServerMetrics.gaugeMergeOpenFileAvailable.set(ShuffleMergeManager.openFileSemaphore.availablePermits());
    this.getSegmentV2 = serverConf.get(SERVER_MERGE_GET_SEGMENT_V2);
    LOG.warn("getSegmentV2 is {}", getSegmentV2);
  }

  public StatusCode registerShuffle(String appId, int shuffleId, String keyClassName, String valueClassName,
                                    String comparatorClassName, int mergedBlockSize) {
    try {
      Class kClass = ClassUtils.getClass(keyClassName);
      Class vClass = ClassUtils.getClass(valueClassName);
      Comparator comparator;
      if (StringUtils.isNotBlank(comparatorClassName)) {
        Constructor constructor = ClassUtils.getClass(comparatorClassName).getDeclaredConstructor();
        constructor.setAccessible(true);
        comparator = (Comparator) constructor.newInstance();
      } else {
        comparator = defaultComparator;
      }
      this.entities.putIfAbsent(appId, JavaUtils.newConcurrentMap());
      this.entities.get(appId).putIfAbsent(shuffleId,
          new ShuffleEntity(serverConf, eventHandler, taskManager, localDirs, appId, shuffleId, kClass, vClass,
              comparator, mergedBlockSize));
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException |
             InvocationTargetException e) {
      LOG.info("Cant register shuffle, caused by ", e);
      unRegisterShuffle(appId, shuffleId);
      return StatusCode.INTERNAL_ERROR;
    }
    return StatusCode.SUCCESS;
  }

  public void unRegisterShuffle(String appId, int shuffleId) {
    if (this.entities.containsKey(appId)) {
      if (this.entities.get(appId).containsKey(shuffleId)) {
        this.entities.get(appId).get(shuffleId).cleanup();
        this.entities.get(appId).remove(shuffleId);
      }
      if (this.entities.get(appId).size() == 0) {
        this.entities.remove(appId);
      }
    }
  }

  public void reportUniqueBlockIds(String appId, int shuffleId, int partitionId,
                                   Roaring64NavigableMap expectedBlockIdMap) throws IOException {
    Map<Integer, ShuffleEntity> entityMap = this.entities.get(appId);
    if (entityMap != null) {
      ShuffleEntity shuffleEntity = entityMap.get(shuffleId);
      if (shuffleEntity != null) {
        shuffleEntity.reportUniqueBlockIds(partitionId, expectedBlockIdMap);
      }
    }
  }

  public void processEvent(MergeEvent event) {
    try {
      Pair<List<AbstractSegment>, Integer> pairs = getSegmentV2 ?
          this.entities.get(event.getAppId()).get(event.getShuffleId()).getPartitionEntity(event.getPartitionId())
              .getSegmentsV2(serverConf, event.getExpectedBlockIdMap().iterator(), event.getKeyClass(),
                  event.getValueClass()) :
          this.entities.get(event.getAppId()).get(event.getShuffleId()).getPartitionEntity(event.getPartitionId())
              .getSegments(serverConf, event.getExpectedBlockIdMap().iterator(), event.getKeyClass(),
                  event.getValueClass());
      this.entities.get(event.getAppId()).get(event.getShuffleId())
          .merge(event.getPartitionId(), pairs.getLeft(), pairs.getRight());
    } catch (Exception e) {
      LOG.info("Found exception when merge, caused by ", e);
      throw new RssException(e);
    }
  }

  public ShuffleDataResult getShuffleData(String appId, int shuffleId, int partitionId, long blockId)
      throws IOException {
    return this.getPartitionEntity(appId, shuffleId, partitionId).getShuffleData(blockId);
  }

  public void cacheBlock(String appId, int shuffleId, ShufflePartitionedData spd) {
    if (this.entities.containsKey(appId) && this.entities.get(appId).containsKey(shuffleId)) {
      this.entities.get(appId).get(shuffleId).cacheBlock(spd);
    }
  }

  public Pair<MergeState, Long> tryGetBlock(String appId, int shuffleId, int partitionId, long blockId) {
    return this.getPartitionEntity(appId, shuffleId, partitionId).tryGetBlock(blockId);
  }

  public int getBlockCnt(String appId, int shuffleId, int partitionId) {
    return this.getPartitionEntity(appId, shuffleId, partitionId).getBlockCnt();
  }

  @VisibleForTesting
  MergeEventHandler getEventHandler() {
    return eventHandler;
  }

  @VisibleForTesting
  PartitionEntity getPartitionEntity(String appId, int shuffleId, int partitionId) {
    return this.entities.get(appId).get(shuffleId).getPartitionEntity(partitionId);
  }

  public void refreshAppId(String appId) {
    taskManager.refreshAppId(appId + MERGE_APP_SUFFIX);
  }
}
