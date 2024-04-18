package org.apache.uniffle.client.request;

public class RssGetSortedShuffleDataRequest {

  private final String appId;
  private final int shuffleId;
  private final int partitionId;
  private final long blockId;

  public RssGetSortedShuffleDataRequest(
      String appId,
      int shuffleId,
      int partitionId,
      long blockId) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.blockId = blockId;
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

  public long getBlockId() {
    return blockId;
  }
}

