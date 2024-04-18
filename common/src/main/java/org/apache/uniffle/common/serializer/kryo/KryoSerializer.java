package org.apache.uniffle.common.serializer.kryo;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.serializer.Serializer;
import org.apache.uniffle.common.serializer.SerializerInstance;

public class KryoSerializer extends Serializer {

  private RssConf rssConf;

  public KryoSerializer(RssConf rssConf) {
    this.rssConf = rssConf;
  }

  @Override
  public SerializerInstance newInstance() {
    return new KryoSerializerInstance(rssConf);
  }

  @Override
  public boolean accept(Class<?> c) {
    return true;
  }
}
