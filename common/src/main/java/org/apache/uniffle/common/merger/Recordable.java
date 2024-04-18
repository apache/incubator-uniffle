package org.apache.uniffle.common.merger;

import java.io.IOException;

public interface Recordable {
  boolean record(long written, Flushable flush, boolean force) throws IOException;

  @FunctionalInterface
  interface Flushable {
    void flush() throws IOException;
  }
}
