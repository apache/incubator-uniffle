package org.apache.uniffle.common.serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

public class SeekableInMemoryByteChannel implements SeekableByteChannel {

  private static final int NAIVE_RESIZE_LIMIT = Integer.MAX_VALUE >> 1;

  private byte[] data;
  private final AtomicBoolean closed = new AtomicBoolean();
  private int position, size;

  /**
   * Constructor taking a byte array.
   *
   * <p>This constructor is intended to be used with pre-allocated buffer or when
   * reading from a given byte array.</p>
   *
   * @param data input data or pre-allocated array.
   */
  public SeekableInMemoryByteChannel(byte[] data) {
    this.data = data;
    size = data.length;
  }

  @Override
  public long position() {
    return position;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    ensureOpen();
    if (newPosition < 0L || newPosition > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Position has to be in range 0.. " + Integer.MAX_VALUE);
    }
    position = (int) newPosition;
    return this;
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public SeekableByteChannel truncate(long newSize) {
    if (size > newSize) {
      size = (int) newSize;
    }
    repositionIfNecessary();
    return this;
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    ensureOpen();
    repositionIfNecessary();
    int wanted = buf.remaining();
    int possible = size - position;
    if (possible <= 0) {
      return -1;
    }
    if (wanted > possible) {
      wanted = possible;
    }
    buf.put(data, position, wanted);
    position += wanted;
    return wanted;
  }

  @Override
  public void close() {
    closed.set(true);
  }

  @Override
  public boolean isOpen() {
    return !closed.get();
  }

  @Override
  public int write(ByteBuffer b) throws IOException {
    ensureOpen();
    int wanted = b.remaining();
    int possibleWithoutResize = size - position;
    if (wanted > possibleWithoutResize) {
      int newSize = position + wanted;
      if (newSize < 0) { // overflow
        resize(Integer.MAX_VALUE);
        wanted = Integer.MAX_VALUE - position;
      } else {
        resize(newSize);
      }
    }
    b.get(data, position, wanted);
    position += wanted;
    if (size < position) {
      size = position;
    }
    return wanted;
  }

  /**
   * Obtains the array backing this channel.
   *
   * <p>NOTE:
   * The returned buffer is not aligned with containing data, use
   * {@link #size()} to obtain the size of data stored in the buffer.</p>
   *
   * @return internal byte array.
   */
  public byte[] array() {
    return data;
  }

  private void resize(int newLength) {
    int len = data.length;
    if (len <= 0) {
      len = 1;
    }
    if (newLength < NAIVE_RESIZE_LIMIT) {
      while (len < newLength) {
        len <<= 1;
      }
    } else { // avoid overflow
      len = newLength;
    }
    data = Arrays.copyOf(data, len);
  }

  private void ensureOpen() throws ClosedChannelException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
  }

  private void repositionIfNecessary() {
    if (position > size) {
      position = size;
    }
  }
}
