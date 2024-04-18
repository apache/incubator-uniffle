package org.apache.uniffle.server.merge;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class MergeEvent {

  private final String appId;
  private final int shuffleId;
  private final int partitionId;
  private final Class kClass;
  private final Class vClass;
  private Roaring64NavigableMap expectedBlockIdMap;

  public MergeEvent(String appId, int shuffleId, int partitionId, Class kClass, Class vClass,
                    Roaring64NavigableMap expectedBlockIdMap) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.kClass = kClass;
    this.vClass =vClass;
    this.expectedBlockIdMap = expectedBlockIdMap;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public Roaring64NavigableMap getExpectedBlockIdMap() {
    return expectedBlockIdMap;
  }

  public Class getKeyClass() {
    return kClass;
  }

  public Class getValueClass() {
    return vClass;
  }

  @Override
  public String toString() {
    return "MergeEvent{" +
        "appId='" + appId + '\'' +
        ", shuffleId=" + shuffleId +
        ", partitionId=" + partitionId +
        ", kClass=" + kClass +
        ", vClass=" + vClass +
        ", expectedBlockIdMap=" + expectedBlockIdMap +
        '}';
  }
}
