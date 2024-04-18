package org.apache.uniffle.common.records;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.serializer.DeserializationStream;
import org.apache.uniffle.common.serializer.PartialInputStream;
import org.apache.uniffle.common.serializer.Serializer;
import org.apache.uniffle.common.serializer.SerializerFactory;
import org.apache.uniffle.common.serializer.SerializerInstance;

public class RecordsReader<K, V> {

  RssConf rssConf;
  PartialInputStream input;

  Class<K> keyClass;
  Class<V> valueClass;

  DeserializationStream<K, V> stream;
  K currentKey;
  V currentValue;

  public RecordsReader(RssConf rssConf, PartialInputStream input, Class<K> keyClass, Class<V> valueClass) {
    this.rssConf = rssConf;
    this.input = input;
    this.keyClass = keyClass;
    this.valueClass = valueClass;

    // Serialization
    SerializerFactory factory = new SerializerFactory(rssConf);
    Serializer serializer = factory.getSerializer(keyClass);
    assert factory.getSerializer(valueClass).getClass().equals(serializer.getClass());
    SerializerInstance instance = serializer.newInstance();
    stream = instance.deserializeStream(this.input, keyClass, valueClass);
  }

  public boolean hasNext() throws IOException {
    return stream.available() > 0;
  }

  public void next() throws IOException {
    this.currentKey = stream.readKey();
    this.currentValue = stream.readValue();
  }

  public long getTotalBytesRead() {
    return stream.getTotalBytesRead();
  }

  public V getCurrentValue() {
    return currentValue;
  }

  public K getCurrentKey() {
    return currentKey;
  }

  public void close() throws IOException {
    if (stream != null) {
      stream.close();
      stream = null;
    }
  }

  @VisibleForTesting
  public DeserializationStream<K, V> getStream() {
    return stream;
  }
}
