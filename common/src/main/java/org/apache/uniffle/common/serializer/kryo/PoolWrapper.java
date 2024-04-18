package org.apache.uniffle.common.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.pool.KryoCallback;
import com.esotericsoftware.kryo.pool.KryoPool;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.config.RssConf;

public class PoolWrapper implements KryoPool {

  private KryoPool pool;
  private final boolean registrationRequired;
  private final boolean referenceTracking;

  public PoolWrapper(RssConf conf) {
    this.pool = newKryoPool();
    this.registrationRequired = conf.getBoolean(RssBaseConf.RSS_KRYO_REGISTRATION_REQUIRED);
    this.referenceTracking = conf.getBoolean(RssBaseConf.RSS_KRYO_REFERENCE_TRACKING);
  }

  private KryoPool newKryoPool() {
    return new Builder(() -> newKryo()).softReferences().build();
  }

  public Kryo newKryo() {
    Kryo kryo = new Kryo();
    kryo.setRegistrationRequired(this.registrationRequired);
    kryo.setReferences(this.referenceTracking);
    return kryo;
  }

  @Override
  public Kryo borrow() {
    return pool.borrow();
  }

  @Override
  public void release(Kryo kryo) {
    pool.release(kryo);
  }

  @Override
  public <T> T run(KryoCallback<T> callback) {
    return pool.run(callback);
  }

  public void reset() {
    pool = newKryoPool();
  }
}
