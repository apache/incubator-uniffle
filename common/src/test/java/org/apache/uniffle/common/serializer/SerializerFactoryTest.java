package org.apache.uniffle.common.serializer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.serializer.kryo.KryoSerializer;
import org.apache.uniffle.common.serializer.writable.WritableSerializer;
import org.junit.jupiter.api.Test;

public class SerializerFactoryTest {

  private static RssConf conf = new RssConf();

  @Test
  public void testGetSerializer() {
    SerializerFactory factory = new SerializerFactory(conf);
    // 1 Test whether it is null
    assertNotNull(factory.getSerializer(Writable.class));
    assertNotNull(factory.getSerializer(IntWritable.class));
    assertNotNull(factory.getSerializer(SerializerUtils.SomeClass.class));
    assertNotNull(factory.getSerializer(String.class));
    assertNotNull(factory.getSerializer(int.class));

    // 2 Check whether the type serializer is right
    assertInstanceOf(WritableSerializer.class, factory.getSerializer(Writable.class));
    assertInstanceOf(WritableSerializer.class, factory.getSerializer(IntWritable.class));
    assertInstanceOf(KryoSerializer.class, factory.getSerializer(SerializerUtils.SomeClass.class));
    assertInstanceOf(KryoSerializer.class, factory.getSerializer(String.class));
    assertInstanceOf(KryoSerializer.class, factory.getSerializer(int.class));

    // 2 Check whether the serializer is cached
    Serializer serializer = factory.getSerializer(Writable.class);
    assertEquals(serializer, factory.getSerializer(IntWritable.class));
    serializer = factory.getSerializer(SerializerUtils.SomeClass.class);
    assertEquals(serializer, factory.getSerializer(String.class));
    assertEquals(serializer, factory.getSerializer(int.class));
  }
}
