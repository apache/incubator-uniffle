package org.apache.uniffle.coordinator.conf;

import java.util.Map;

import org.apache.uniffle.common.RemoteStorageInfo;

/**
 * This class is to hold the dynamic conf,
 * which includes the rss conf for the client and the remote storage hadoop configs.
 */
public class ClientConf {
  private Map<String, String> rssClientConf;

  // key:remote-path, val: storage-conf
  private Map<String, RemoteStorageInfo> remoteStorageInfos;

  public ClientConf(Map<String, String> rssClientConf) {
    this.rssClientConf = rssClientConf;
  }

  public ClientConf(Map<String, String> rssClientConf,
      Map<String, RemoteStorageInfo> remoteStorageInfos) {
    this.rssClientConf = rssClientConf;
    this.remoteStorageInfos = remoteStorageInfos;
  }

  public Map<String, String> getRssClientConf() {
    return rssClientConf;
  }

  public void setRssClientConf(Map<String, String> rssClientConf) {
    this.rssClientConf = rssClientConf;
  }

  public Map<String, RemoteStorageInfo> getRemoteStorageInfos() {
    return remoteStorageInfos;
  }

  public void setRemoteStorageInfos(
      Map<String, RemoteStorageInfo> remoteStorageInfos) {
    this.remoteStorageInfos = remoteStorageInfos;
  }
}
