package org.apache.uniffle.common.serializer.writable;

import java.io.DataInputStream;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.uniffle.common.serializer.DeserializationStream;
import org.apache.uniffle.common.serializer.PartialInputStream;

public class WritableDeserializationStream<K extends Writable, V extends Writable> extends DeserializationStream<K, V> {

  private DataInputStream dataIn;

  public WritableDeserializationStream(WritableSerializerInstance instance, PartialInputStream inputStream,
                                       Class<K> keyClass, Class<V> valueClass) {
    super(inputStream, keyClass, valueClass);
    this.dataIn = new DataInputStream(inputStream);
  }

  @Override
  public K readKey() throws IOException {
    Writable writable = ReflectionUtils.newInstance(keyClass, null);
    writable.readFields(dataIn);
    return (K) writable;
  }

  @Override
  public V readValue() throws IOException {
    Writable writable = ReflectionUtils.newInstance(valueClass, null);
    writable.readFields(dataIn);
    return (V) writable;
  }

  @Override
  public long available() throws IOException {
    return inputStream.available();
  }

  @Override
  public long getTotalBytesRead() {
    return inputStream.getPos();
  }

  @Override
  public void close() throws IOException {
    if (dataIn != null) {
      dataIn.close();
      dataIn = null;
    }
  }
}
