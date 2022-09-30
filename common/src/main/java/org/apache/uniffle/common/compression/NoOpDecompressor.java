package org.apache.uniffle.common.compression;

import java.nio.ByteBuffer;

public class NoOpDecompressor implements Decompressor {

  @Override
  public void decompress(ByteBuffer src, int uncompressedLen, ByteBuffer dest, int destOffset) {
    dest.put(src);
  }
}
