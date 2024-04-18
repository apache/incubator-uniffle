package org.apache.uniffle.common.merger;

import java.io.IOException;

public abstract class AbstractSegment <K, V> {

  private long id;

  public AbstractSegment(long id) {
    this.id = id;
  }

  public abstract boolean hasNext() throws IOException;

  public abstract void next() throws IOException;

  public abstract K getCurrentKey();

  public abstract V getCurrentValue();

  public long getId() {
    return this.id;
  }

  public abstract void close() throws IOException;
}
