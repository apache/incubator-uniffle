package org.apache.uniffle.common.serializer;

import java.io.IOException;
import java.io.InputStream;

/*
 * PartialInputStream is a configurable partial input stream, which
 * only allows reading from start to end of the source input stream.
 * */
public abstract class PartialInputStream extends InputStream {

  @Override
  public abstract int available() throws IOException;

  public abstract long getStart();

  public abstract long getEnd();

  public abstract long getPos();
}
