package org.apache.uniffle.coordinator.web.request;

import java.util.List;

public class DecommissionRequest {
  private List<String> serverIds;

  public List<String> getServerIds() {
    return serverIds;
  }

  public void setServerIds(List<String> serverIds) {
    this.serverIds = serverIds;
  }

}
