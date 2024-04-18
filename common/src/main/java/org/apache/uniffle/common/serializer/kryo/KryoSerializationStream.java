package org.apache.uniffle.common.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.uniffle.common.serializer.SerializationStream;

public class KryoSerializationStream<K, V> extends SerializationStream<K, V> {

  private final KryoSerializerInstance instance;
  private Output output;
  private Kryo kryo;

  public KryoSerializationStream(KryoSerializerInstance instance, OutputStream out) {
    this.instance = instance;
    this.output = instance.isUnsafe() ? new UnsafeOutput(out) : new Output(out);
    this.kryo = instance.borrowKryo();
  }

  @Override
  public SerializationStream writeKey(K key) throws IOException {
    kryo.writeClassAndObject(output, key);
    return this;
  }

  @Override
  public SerializationStream writeValue(V value) throws IOException {
    kryo.writeClassAndObject(output, value);
    return this;
  }

  @Override
  public void flush() throws IOException {
    if (output == null) {
      throw new IOException("Stream is closed");
    }
    output.flush();
  }

  @Override
  public void close() {
    if (output != null) {
      try {
        output.close();
      } finally {
        this.instance.releaseKryo(kryo);
        kryo = null;
        output = null;
      }
    }
  }

  @Override
  public long getTotalBytesWritten() {
    return output.total();
  }
}
