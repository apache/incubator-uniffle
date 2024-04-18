package org.apache.uniffle.client.shuffle.reader;

import java.io.IOException;

public abstract class KeyValueReader<K, V> {

  public abstract boolean next() throws IOException;

  public abstract K getCurrentKey() throws IOException;

  public abstract V getCurrentValue() throws IOException;
}
