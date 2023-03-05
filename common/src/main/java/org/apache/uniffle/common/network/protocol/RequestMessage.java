package org.apache.uniffle.common.network.protocol;

import io.netty.buffer.ByteBuf;

public interface RequestMessage {

  void serialize(ByteBuf buf);

  int size();

  long getRequestId();
}
