package org.apache.uniffle.common.compression;

/**
 * The class is to be as the basic compressor for test cases
 */
public class NoOpCompressor implements Compressor {

  @Override
  public byte[] compress(byte[] data) {
    byte[] dst = new byte[data.length];
    System.arraycopy(data, 0, dst, 0, data.length);
    return dst;
  }
}
