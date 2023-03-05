package org.apache.uniffle.common.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.uniffle.common.exception.RssException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Future;

public class SocketUtils {
  public static byte[] readBytes(InputStream stream, int numBytes) {
    if (numBytes == 0) {
      return new byte[0];
    }

    byte[] result = new byte[numBytes];
    int readBytes = 0;
    while (readBytes < numBytes) {
      try {
        int numBytesToRead = numBytes - readBytes;
        int count = stream.read(result, readBytes, numBytesToRead);

        if (count == -1) {
          throw new RssException(
              "Failed to read data bytes due to end of stream: "
                  + numBytesToRead);
        }

        readBytes += count;
      } catch (IOException e) {
        throw new RssException("Failed to read data", e);
      }
    }

    return result;
  }

  public static int readInt(InputStream stream) {
    byte[] bytes = readBytes(stream, 4);
    ByteBuf buf = Unpooled.wrappedBuffer(bytes);
    try {
      int value = buf.readInt();
      return value;
    } finally {
      buf.release();
    }
  }

  public static long readLong(InputStream stream) {
    byte[] bytes = readBytes(stream, 8);
    ByteBuf buf = Unpooled.wrappedBuffer(bytes);
    try {
      long value = buf.readLong();
      return value;
    } finally {
      buf.release();
    }
  }

  public static String readLengthAndString(InputStream stream) {
    int len = readInt(stream);
    if (len < 0) {
      return null;
    }

    byte[] bytes = readBytes(stream, len);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  public static ByteBuffer readAsyncSocket(AsynchronousSocketChannel socket, int numBytes) {
    if (numBytes == 0) {
      return ByteBuffer.allocate(0);
    }

    ByteBuffer byteBuffer = ByteBuffer.allocate(numBytes);

    while (byteBuffer.position() < numBytes) {
      Future<Integer> future = socket.read(byteBuffer);
      try {
        future.get();
      } catch (Throwable e) {
        throw new RuntimeException("Failed to read async socket", e);
      }
    }

    byteBuffer.rewind();

    return byteBuffer;
  }
}
