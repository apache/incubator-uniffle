package org.apache.uniffle.common.merger;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import java.io.File;
import java.io.IOException;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.records.RecordsReader;
import org.apache.uniffle.common.serializer.PartialInputStream;

public class Segment<K, V> extends AbstractSegment<K, V> {

  private RecordsReader<K, V> reader;
  ByteBuf byteBuf = null;

  public Segment(RssConf rssConf, ByteBuf byteBuf, long blockId, Class<K> keyClass, Class<V> valueClass)
      throws IOException {
    super(blockId);
    this.byteBuf = byteBuf;
    this.byteBuf.retain();
    byte[] buffer = byteBuf.array();
    this.reader = new RecordsReader<>(rssConf, PartialInputStream.newInputStream(buffer, 0, buffer.length),
        keyClass, valueClass);
  }

  // The buffer must be sorted by key
  public Segment(RssConf rssConf, byte[] buffer, long blockId, Class<K> keyClass, Class<V> valueClass)
      throws IOException {
    super(blockId);
    this.reader =
        new RecordsReader<>(rssConf, PartialInputStream.newInputStream(buffer, 0, buffer.length), keyClass, valueClass);
  }

  public Segment(RssConf rssConf, File file, long start, long end, long blockId, Class<K> keyClass, Class<V> valueClass)
      throws IOException {
    super(blockId);
    this.reader = new RecordsReader<K, V>(rssConf, PartialInputStream.newInputStream(file, start, end), keyClass,
        valueClass);
  }

  @Override
  public boolean hasNext() throws IOException {
    return this.reader.hasNext();
  }

  @Override
  public void next() throws IOException {
    this.reader.next();
  }

  @Override
  public K getCurrentKey() {
    return this.reader.getCurrentKey();
  }

  @Override
  public V getCurrentValue() {
    return this.reader.getCurrentValue();
  }

  @Override
  public void close() throws IOException {
    if (byteBuf != null) {
      this.byteBuf.release();
      this.byteBuf = null;
    }
    if (this.reader != null) {
      this.reader.close();
      this.reader = null;
    }
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    // Prevents ByteBuf memory leaks caused by forgetting close
    if (this.byteBuf != null) {
      this.byteBuf.release();
      this.byteBuf = null;
    }
  }

  @VisibleForTesting
  public RecordsReader<K, V> getReader() {
    return reader;
  }
}
