package org.apache.uniffle.common.serializer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

public abstract class SerializerInstance {

  public abstract <T> DataOutputBuffer serialize(T t) throws IOException;

  public abstract <T> T deserialize(DataInputBuffer buffer , Class vClass) throws IOException;

  public abstract <K, V> SerializationStream serializeStream(OutputStream output);

  public abstract <K, V> DeserializationStream deserializeStream(PartialInputStream input, Class<K> keyClass,
                                                                 Class<V> valueClass);
}
