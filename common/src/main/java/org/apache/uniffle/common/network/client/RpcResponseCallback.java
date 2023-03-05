package org.apache.uniffle.common.network.client;

import java.nio.ByteBuffer;

public interface RpcResponseCallback {
  /**
   * Successful serialized result from server.
   *
   * <p>After `onSuccess` returns, `response` will be recycled and its content will become invalid.
   * Please copy the content of `response` if you want to use it after `onSuccess` returns.
   */
  void onSuccess(Object object);

  /** Exception either propagated from server or raised on client side. */
  void onFailure(Throwable e);
}