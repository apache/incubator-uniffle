package org.apache.uniffle.client.shuffle.reader;

import java.io.IOException;
import org.apache.uniffle.client.shuffle.RecordBuffer;
import org.apache.uniffle.common.merger.AbstractSegment;

public class BufferedSegment<K, V> extends AbstractSegment<K, V> {

  private RecordBuffer<K, V> recordBuffer;
  private int index = -1;

  public BufferedSegment(RecordBuffer recordBuffer) {
    super(recordBuffer.getPartitionId());
    this.recordBuffer = recordBuffer;
  }

  @Override
  public boolean hasNext() throws IOException {
    return index < this.recordBuffer.size() - 1;
  }

  @Override
  public void next() throws IOException {
    index++;
  }

  @Override
  public K getCurrentKey() {
    return this.recordBuffer.getKey(index);
  }

  @Override
  public V getCurrentValue() {
    return this.recordBuffer.getValue(index);
  }

  @Override
  public void close() throws IOException {
    if (recordBuffer != null) {
      this.recordBuffer.clear();
      this.recordBuffer = null;
    }
  }
}
