package org.apache.uniffle.common.merger;

import java.io.IOException;

public interface KeyValueIterator<K, V> {

  K getCurrentKey();

  V getCurrentValue();

  boolean next() throws IOException;

  void close() throws IOException;
}
