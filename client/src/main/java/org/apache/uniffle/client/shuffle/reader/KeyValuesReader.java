package org.apache.uniffle.client.shuffle.reader;

import java.io.IOException;

public abstract class KeyValuesReader<K, V> {

  public abstract boolean next() throws IOException;

  public abstract K getCurrentKey() throws IOException;

  public abstract Iterable<V> getCurrentValues() throws IOException;
}
