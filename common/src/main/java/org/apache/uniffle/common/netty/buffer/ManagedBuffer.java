package org.apache.uniffle.common.netty.buffer;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;

public abstract class ManagedBuffer {

  public abstract int size();

  public abstract ByteBuf byteBuf();

  public abstract ByteBuffer nioByteBuffer();

  public abstract ManagedBuffer release();

  /**
   * Convert the buffer into an Netty object, used to write the data out. The return value is either
   * a {@link io.netty.buffer.ByteBuf} or a {@link io.netty.channel.FileRegion}.
   */
  public abstract Object convertToNetty();
}
