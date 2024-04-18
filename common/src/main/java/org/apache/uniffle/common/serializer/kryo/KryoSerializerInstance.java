package org.apache.uniffle.common.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.serializer.DeserializationStream;
import org.apache.uniffle.common.serializer.PartialInputStream;
import org.apache.uniffle.common.serializer.SerializationStream;
import org.apache.uniffle.common.serializer.SerializerInstance;
import org.apache.uniffle.common.serializer.PartialInputStream;

public class KryoSerializerInstance extends SerializerInstance {

  private boolean unsafe;
  private boolean usePool;
  private volatile Kryo cachedKryo = null;
  private PoolWrapper pool;

  public KryoSerializerInstance(RssConf rssConf) {
    this.pool = new PoolWrapper(rssConf);
    this.unsafe = rssConf.getBoolean(RssBaseConf.RSS_KRYO_UNSAFE);
    this.usePool = rssConf.getBoolean(RssBaseConf.RSS_USE_POOL);
  }

  public Kryo borrowKryo() {
    if (usePool) {
      Kryo kryo = pool.borrow();
      kryo.reset();
      // TODO: To remove, use isolated class loader
      kryo.register(scala.Tuple2.class, new com.twitter.chill.Tuple2Serializer());
      return kryo;
    } else {
      if (cachedKryo != null) {
        Kryo kryo = cachedKryo;
        kryo.reset();
        cachedKryo = null;
        return kryo;
      } else {
        return pool.newKryo();
      }
    }
  }

  void releaseKryo(Kryo kryo) {
    if (usePool) {
      pool.release(kryo);
    } else {
      if (cachedKryo == null) {
        cachedKryo = kryo;
      }
    }
  }

  public boolean isUnsafe() {
    return unsafe;
  }

  @Override
  public <T> DataOutputBuffer serialize(T t) {
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
