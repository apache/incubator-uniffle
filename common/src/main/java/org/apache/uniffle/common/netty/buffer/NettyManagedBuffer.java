package org.apache.uniffle.common.netty.buffer;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class NettyManagedBuffer extends ManagedBuffer {

  private ByteBuf buf;

  public NettyManagedBuffer(ByteBuf byteBuf) {
    this.buf = byteBuf;
  }

  @Override
  public int size() {
    return buf.readableBytes();
  }

  @Override
  public ByteBuf byteBuf() {
    return Unpooled.wrappedBuffer(this.nioByteBuffer());
  }

  @Override
  public ByteBuffer nioByteBuffer() {
    return buf.nioBuffer();
  }

  @Override
  public ManagedBuffer release() {
    buf.release();
    return this;
  }

  @Override
  public Object convertToNetty() {
    return buf.duplicate().retain();
  }
}
