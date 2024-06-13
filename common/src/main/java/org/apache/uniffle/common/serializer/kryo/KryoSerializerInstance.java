package org.apache.uniffle.common.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.commons.lang3.ClassUtils;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.serializer.DeserializationStream;
import org.apache.uniffle.common.serializer.PartialInputStream;
import org.apache.uniffle.common.serializer.SerializationStream;
import org.apache.uniffle.common.serializer.SerializerInstance;

public class KryoSerializerInstance extends SerializerInstance {

  private boolean unsafe;
  private PoolWrapper pool;

  public KryoSerializerInstance(RssConf rssConf) {
    this.pool = new PoolWrapper(rssConf);
    this.unsafe = rssConf.getBoolean(RssBaseConf.RSS_KRYO_UNSAFE);
  }

  public Kryo borrowKryo() {
    Kryo kryo = pool.borrow();
    kryo.reset();
    try {
      Class cls = ClassUtils.getClass("com.twitter.chill.AllScalaRegistrar");
      Object obj = cls.newInstance();
      Method method = cls.getDeclaredMethod("apply", Kryo.class);
      kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
      method.invoke(obj, kryo);
    } catch (ClassNotFoundException | InvocationTargetException | InstantiationException | IllegalAccessException |
             NoSuchMethodException e) {
      throw new RssException(e);
    }
    return kryo;
  }

  void releaseKryo(Kryo kryo) {
    pool.release(kryo);
  }

  public boolean isUnsafe() {
    return unsafe;
  }

  @Override
  public <T> void serialize(T t, DataOutputStream out) throws IOException {
    throw new RssException("not supportted!");
  }

  @Override
  public <T> T deserialize(DataInputBuffer buffer, Class vClass) {
    throw new RssException("not supportted!");
  }

  @Override
  public <K, V> SerializationStream serializeStream(OutputStream output) {
    return new KryoSerializationStream(this, output);
  }

  @Override
  public <K, V> DeserializationStream deserializeStream(PartialInputStream input, Class<K> keyClass,
                                                        Class<V> valueClass) {
    return new KryoDeserializationStream(this, input, keyClass, valueClass);
  }
}
