package org.apache.uniffle.common.message;

import io.netty.buffer.ByteBuf;

public abstract class SerializableMessage {
  public abstract void serialize(ByteBuf buf);
}
