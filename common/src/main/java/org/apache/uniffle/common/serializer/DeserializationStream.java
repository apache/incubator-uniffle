package org.apache.uniffle.common.serializer;

import java.io.IOException;

public abstract class DeserializationStream<K, V> {

  protected PartialInputStream inputStream;
  protected Class<K> keyClass;
  protected Class<V> valueClass;

  public DeserializationStream(PartialInputStream inputStream, Class<K> keyClass, Class<V> valueClass) {
    this.inputStream = inputStream;
    this.keyClass = keyClass;
    this.valueClass = valueClass;
  }

  public abstract K readKey() throws IOException;

  public abstract V readValue() throws IOException;

  public abstract long available() throws IOException;

  // Return read offset
  public abstract long getTotalBytesRead();

  public abstract void close() throws IOException;
}
