package org.apache.uniffle.common.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.UnsafeInput;
import java.io.EOFException;
import java.io.IOException;
import java.util.Locale;
import org.apache.uniffle.common.serializer.DeserializationStream;
import org.apache.uniffle.common.serializer.PartialInputStream;

public class KryoDeserializationStream<K, V> extends DeserializationStream<K, V> {

  private final KryoSerializerInstance instance;
  private Input input;
  private Kryo kryo;
  private long seekPos;

  public KryoDeserializationStream(KryoSerializerInstance instance, PartialInputStream inputStream,
                                   Class<K> keyClass, Class<V> valueClass) {
    super(inputStream, keyClass, valueClass);
    this.instance = instance;
    this.input = instance.isUnsafe() ? new UnsafeInput(inputStream) : new Input(inputStream);
    this.kryo = instance.borrowKryo();
    this.seekPos = inputStream.getStart();
  }

  @Override
  public K readKey() throws IOException {
    try {
      K k = (K) kryo.readClassAndObject(input);
      return k;
    } catch (KryoException e) {
      if (e.getMessage().toLowerCase(Locale.ROOT).contains("buffer underflow")) {
        throw new EOFException();
      }
      throw e;
    }
  }

  @Override
  public V readValue() throws IOException {
    try {
      V v = (V) kryo.readClassAndObject(input);
      return v;
    } catch (KryoException e) {
      if (e.getMessage().toLowerCase(Locale.ROOT).contains("buffer underflow")) {
        throw new EOFException();
      }
      throw e;
    }
  }

  @Override
  public long available() throws IOException {
    return inputStream.getEnd() - getTotalBytesRead();
  }

  // kryo use buffer to read inputStream, so we should use real read offset by input.total()
  @Override
  public long getTotalBytesRead() {
    return seekPos + input.total();
  }

  @Override
  public void close() {
    if (input != null) {
      try {
        input.close();
      } finally {
        instance.releaseKryo(kryo);
        kryo = null;
        input = null;
      }
    }
  }
}
