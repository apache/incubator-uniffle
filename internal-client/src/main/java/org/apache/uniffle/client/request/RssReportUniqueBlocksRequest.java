package org.apache.uniffle.client.request;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class RssReportUniqueBlocksRequest {

  private String appId;
  private int shuffleId;
  private int partitionId;
  private Roaring64NavigableMap expectedBlockIds;

  public RssReportUniqueBlocksRequest(String appId, int shuffleId, int partitionId,
                                      Roaring64NavigableMap expectedBlockIds){
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.expectedBlockIds = expectedBlockIds;
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

  public Roaring64NavigableMap getExpectedTaskIds() {
    return expectedBlockIds;
  }
}
