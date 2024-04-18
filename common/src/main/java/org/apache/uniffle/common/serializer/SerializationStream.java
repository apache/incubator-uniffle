package org.apache.uniffle.common.serializer;

import java.io.IOException;

public abstract class SerializationStream<K, V> {

  public abstract SerializationStream writeKey(K key) throws IOException;

  public abstract SerializationStream writeValue(V value) throws IOException;

  public abstract void flush() throws IOException;

  public abstract void close() throws IOException;

  public abstract long getTotalBytesWritten();
}
