package org.apache.uniffle.common.network.protocol;

import io.netty.buffer.ByteBuf;

public interface ResponseMessage {
  void deserialize(ByteBuf buf);
  void serialize(ByteBuf buf);
  long getRequestId();
  int getMessageType();
}
