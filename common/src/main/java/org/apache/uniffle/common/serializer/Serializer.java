package org.apache.uniffle.common.serializer;

public abstract class Serializer {

  public abstract SerializerInstance newInstance();

  public abstract boolean accept(Class<?> c);
}
