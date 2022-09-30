package org.apache.uniffle.common.compression;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.uniffle.common.config.RssConf;

import static org.apache.uniffle.common.config.RssClientConf.COMPRESSION_TYPE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class CompressionTest {

  static List<Arguments> testCompression() {
    int[] sizes = {1, 1024, 128 * 1024, 512 * 1024, 1024 * 1024, 4 * 1024 * 1024};
    CompressionFactory.Type[] types = {CompressionFactory.Type.ZSTD, CompressionFactory.Type.LZ4};

    List<Arguments> arguments = new ArrayList<>();
    for (int size : sizes) {
      for (CompressionFactory.Type type : types) {
        arguments.add(
            Arguments.of(size, type)
        );
      }
    }
    return arguments;
  }

  @ParameterizedTest
  @MethodSource
  public void testCompression(int size, CompressionFactory.Type type) {
    byte[] data = RandomUtils.nextBytes(size);
    RssConf conf = new RssConf();
    conf.set(COMPRESSION_TYPE, type);
        
    // case1: heap bytebuffer
    Compressor compressor = CompressionFactory.of().getCompressor(conf);
    byte[] compressed = compressor.compress(data);

    Decompressor decompressor = CompressionFactory.of().getDecompressor(conf);
    ByteBuffer dest = ByteBuffer.allocate(size);
    decompressor.decompress(ByteBuffer.wrap(compressed), size, dest, 0);

    assertArrayEquals(data, dest.array());

    // case2: non-heap bytebuffer
    ByteBuffer src = ByteBuffer.allocateDirect(compressed.length);
    src.put(compressed);
    src.flip();
    ByteBuffer dst = ByteBuffer.allocateDirect(size);
    decompressor.decompress(src, size, dst, 0);
    byte[] res = new byte[size];
    dst.get(res);
    assertArrayEquals(data, res);

    // case3: use the recycled bytebuffer
    ByteBuffer recycledDst = ByteBuffer.allocate(size + 10);
    decompressor.decompress(ByteBuffer.wrap(compressed), size, recycledDst, 0);
    recycledDst.get(res);
    assertArrayEquals(data, res);
  }
}
