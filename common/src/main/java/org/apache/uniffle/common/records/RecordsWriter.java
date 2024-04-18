package org.apache.uniffle.common.records;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.serializer.SerializationStream;
import org.apache.uniffle.common.serializer.Serializer;
import org.apache.uniffle.common.serializer.SerializerFactory;
import org.apache.uniffle.common.serializer.SerializerInstance;

public class RecordsWriter<K, V> {

  RssConf rssConf;
  OutputStream out;
  Class<K> keyClass;
  Class<V> valueClass;

  SerializationStream<K, V> stream;
  long currentOffset = 0;

  public RecordsWriter(RssConf rssConf, OutputStream out, Class<K> keyClass, Class<V> valueClass) {
    this.rssConf = rssConf;
    this.out = out;
    this.keyClass = keyClass;
    this.valueClass = valueClass;

    // Serialization
    SerializerFactory factory = new SerializerFactory(rssConf);
    Serializer serializer = factory.getSerializer(keyClass);
    assert factory.getSerializer(valueClass).getClass().equals(serializer.getClass());
    SerializerInstance instance = serializer.newInstance();
    stream = instance.serializeStream(this.out);
  }

  public void append(K key, V value) throws IOException {
    // write key and value
    stream.writeKey(key);
    stream.writeValue(value);

    // update bytes written
    currentOffset = stream.getTotalBytesWritten();
  }

  public void flush() throws IOException {
    stream.flush();
  }

  public void close() throws IOException {
    if (stream != null) {
      stream.close();
      stream = null;
    }
  }

  public long getTotalBytesWritten() {
    return stream.getTotalBytesWritten();
  }
}
