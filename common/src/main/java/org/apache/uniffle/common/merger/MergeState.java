package org.apache.uniffle.common.merger;

public enum MergeState {

  DONE(0),
  INITED(1),
  MERGING(2),
  INTERNAL_ERROR(3);

  private final int code;

  MergeState(int code) {
    this.code = code;
  }

  public int code() {
    return code;
  }
}
