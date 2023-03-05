package org.apache.uniffle.common.network.client;

import com.google.common.collect.Maps;

import java.util.Map;

public class TransportContext {
  private Map<Long, RpcResponseCallback> callbackMap = Maps.newConcurrentMap();
  public TransportContext() {}

  public void addResponseCallback(long requestId, RpcResponseCallback callback) {
    callbackMap.put(requestId, callback);
  }
  public RpcResponseCallback getResponseCallback(long requestId) {
    return callbackMap.get(requestId);
  }

}
