package org.apache.uniffle.common.serializer;

import static org.apache.uniffle.common.config.RssBaseConf.RSS_IO_SERIALIZATIONS;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SerializerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(SerializerFactory.class);

  private List<Serializer> serializers = new ArrayList<Serializer>();

  RssConf conf;

  public SerializerFactory(RssConf conf) {
    this.conf = conf;
    for (String serializerName : StringUtils.split(conf.get(RSS_IO_SERIALIZATIONS), ",")) {
      add(conf, serializerName);
    }
  }

  private void add(RssConf conf, String serializerName) {
    try {
      Class<? extends Serializer> sClass = (Class<? extends Serializer>) ClassUtils.getClass(serializerName);
      Constructor<? extends Serializer> constructor = sClass.getConstructor(RssConf.class);
      Serializer serializer = constructor.newInstance(conf);
      serializers.add(serializer);
    } catch (Exception e) {
      LOG.warn("Construct Serialization fail, caused by ", e);
      throw new RssException(e);
    }
  }

  public Serializer getSerializer(Class c) {
    for (Serializer serializer : serializers) {
      if (serializer.accept(c)) {
        return serializer;
      }
    }
    return null;
  }
}
