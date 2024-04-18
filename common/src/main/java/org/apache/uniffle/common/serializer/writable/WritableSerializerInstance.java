package org.apache.uniffle.common.serializer.writable;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.serializer.DeserializationStream;
import org.apache.uniffle.common.serializer.PartialInputStream;
import org.apache.uniffle.common.serializer.SerializationStream;
import org.apache.uniffle.common.serializer.SerializerInstance;

public class WritableSerializerInstance extends SerializerInstance {

  public WritableSerializerInstance(RssConf rssConf) {
  }

  @Override
  public <T> DataOutputBuffer serialize(T t) throws IOException {
    DataOutputBuffer dataOut = new DataOutputBuffer();
    ((Writable) t).write(dataOut);
    return dataOut;
  }

  @Override
  public <T> T deserialize(DataInputBuffer buffer, Class vClass) throws IOException {
    Writable writable = (Writable) ReflectionUtils.newInstance(vClass, null);
    writable.readFields(buffer);
    return (T) writable;
  }

  @Override
  public <K, V> SerializationStream serializeStream(OutputStream output) {
    return new WritableSerializationStream(this, output);
  }

  @Override
  public <K, V> DeserializationStream deserializeStream(PartialInputStream input, Class<K> keyClass,
                                                        Class<V> valueClass) {
    return new WritableDeserializationStream(this, input, keyClass, valueClass);
  }
}
