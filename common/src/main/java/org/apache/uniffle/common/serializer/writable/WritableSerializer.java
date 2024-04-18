package org.apache.uniffle.common.serializer.writable;

import org.apache.hadoop.io.Writable;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.serializer.Serializer;
import org.apache.uniffle.common.serializer.SerializerInstance;

public class WritableSerializer extends Serializer {

  private RssConf rssConf;

  public WritableSerializer(RssConf rssConf) {
    this.rssConf = rssConf;
  }

  @Override
  public SerializerInstance newInstance() {
    return new WritableSerializerInstance(rssConf);
  }

  @Override
  public boolean accept(Class<?> c) {
    return Writable.class.isAssignableFrom(c);
  }
}
