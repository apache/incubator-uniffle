package org.apache.uniffle.common.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class ByteBufUtils {
  public static final byte[] convertIntToBytes(int value) {
    byte[] bytes = new byte[Integer.BYTES];
    writeInt(bytes, 0, value);
    return bytes;
  }

  public static final void writeLengthAndString(ByteBuf buf, String str) {
    if (str == null) {
      buf.writeInt(-1);
      return;
    }

    byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
    buf.writeInt(bytes.length);
    buf.writeBytes(bytes);
  }

  public static final String readLengthAndString(ByteBuf buf) {
    int length = buf.readInt();
    if (length == -1) {
      return null;
    }

    byte[] bytes = new byte[length];
    buf.readBytes(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  public static final byte[] readBytes(ByteBuf buf) {
    // TODO a better implementation?
    byte[] bytes = new byte[buf.readableBytes()];
    buf.readBytes(bytes);
    return bytes;
  }

  public static final ByteBuf wrappedBuffer(byte[] bytes) {
    return Unpooled.wrappedBuffer(bytes);
  }

  public static ByteBuf copyDirectBuffer(ByteBuf buf) {
    ByteBuf newByteBuf = Unpooled.directBuffer(buf.readableBytes());
    buf.readBytes(newByteBuf, buf.readableBytes());
    return newByteBuf;
  }

  public static final void readBytesToStream(ByteBuf buf, OutputStream stream) throws IOException {
    final int maxNumBytes = 64000;
    byte[] bytes = new byte[maxNumBytes];
    while (buf.readableBytes() > 0) {
      int numBytes = Math.min(buf.readableBytes(), maxNumBytes);
      buf.readBytes(bytes, 0, numBytes);
      stream.write(bytes, 0, numBytes);
    }
  }

  public static final void writeInt(byte[] bytes, int index, int value) {
    bytes[index] = (byte) (value >>> 24);
    bytes[index + 1] = (byte) (value >>> 16);
    bytes[index + 2] = (byte) (value >>> 8);
    bytes[index + 3] = (byte) value;
  }

  public static final void writeLong(byte[] bytes, int index, long value) {
    bytes[index] = (byte) (value >>> 56);
    bytes[index + 1] = (byte) (value >>> 48);
    bytes[index + 2] = (byte) (value >>> 40);
    bytes[index + 3] = (byte) (value >>> 32);
    bytes[index + 4] = (byte) (value >>> 24);
    bytes[index + 5] = (byte) (value >>> 16);
    bytes[index + 6] = (byte) (value >>> 8);
    bytes[index + 7] = (byte) value;
  }

  public static final int readInt(byte[] bytes, int index) {
    return (bytes[index] & 0xff) << 24 |
               (bytes[index + 1] & 0xff) << 16 |
               (bytes[index + 2] & 0xff) << 8 |
               bytes[index + 3] & 0xff;
  }

  public static final long readLong(byte[] bytes, int index) {
    return ((long) bytes[index] & 0xff) << 56 |
               ((long) bytes[index + 1] & 0xff) << 48 |
               ((long) bytes[index + 2] & 0xff) << 40 |
               ((long) bytes[index + 3] & 0xff) << 32 |
               ((long) bytes[index + 4] & 0xff) << 24 |
               ((long) bytes[index + 5] & 0xff) << 16 |
               ((long) bytes[index + 6] & 0xff) << 8 |
               (long) bytes[index + 7] & 0xff;
  }
}
