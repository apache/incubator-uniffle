package org.apache.uniffle.common.serializer.writable;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.uniffle.common.serializer.SerializationStream;

public class WritableSerializationStream<K extends Writable, V extends Writable> extends SerializationStream<K, V> {

  WritableSerializerInstance instance;
  private OutputStream output;
  private long totalBytesWritten = 0;

  public WritableSerializationStream(WritableSerializerInstance instance, OutputStream out) {
    this.instance = instance;
    this.output = out;
  }

  @Override
  public SerializationStream writeKey(K key) throws IOException {
    DataOutputBuffer buffer = new DataOutputBuffer();
    key.write(buffer);
    buffer.writeTo(output);
    totalBytesWritten += buffer.getLength();
    return this;
  }

  @Override
  public SerializationStream writeValue(V value) throws IOException {
    DataOutputBuffer buffer = new DataOutputBuffer();
    value.write(buffer);
    buffer.writeTo(output);
    totalBytesWritten += buffer.getLength();
    return this;
  }

  @Override
  public void flush() throws IOException {
    output.flush();
  }

  @Override
  public void close() throws IOException {
    if (output != null) {
      output.close();
      output = null;
    }
  }

  @Override
  public long getTotalBytesWritten() {
    return totalBytesWritten;
  }
}
