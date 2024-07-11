package org.apache.uniffle.common.storage;

import java.util.concurrent.atomic.AtomicLong;

public class ApplicationStorageInfo {
  private String appId;
  private AtomicLong fileNum;
  private AtomicLong usedBytes;

  public ApplicationStorageInfo(String appId) {
    this.appId = appId;
    this.fileNum = new AtomicLong();
    this.usedBytes = new AtomicLong();
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public long getFileNum() {
    return fileNum.get();
  }

  public void incFileNum(long fileNum) {
    this.fileNum.addAndGet(fileNum);
  }

  public long getUsedBytes() {
    return usedBytes.get();
  }

  public void incUsedBytes(long usedBytes) {
    this.usedBytes.addAndGet(usedBytes);
  }
}
