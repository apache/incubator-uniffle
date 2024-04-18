package org.apache.uniffle.client.response;

import java.nio.ByteBuffer;
import org.apache.uniffle.common.rpc.StatusCode;

public class RssGetSortedShuffleDataResponse extends ClientResponse {

  private final ByteBuffer data;
  private final long nextBlockId;
  private final int mergeState;

  public RssGetSortedShuffleDataResponse(StatusCode statusCode, ByteBuffer data, long nextBlockId, int mergeState) {
    super(statusCode);
    this.data = data;
    this.nextBlockId = nextBlockId;
    this.mergeState = mergeState;
  }

  public ByteBuffer getData() {
    return data;
  }

  public long getNextBlockId() {
    return nextBlockId;
  }

  public int getMergeState() {
    return mergeState;
  }
}

