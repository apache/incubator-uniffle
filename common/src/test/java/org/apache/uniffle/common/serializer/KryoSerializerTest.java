package org.apache.uniffle.common.serializer;

import static org.apache.uniffle.common.serializer.SerializerUtils.genData;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.serializer.SerializerUtils.SomeClass;
import org.apache.uniffle.common.serializer.kryo.KryoSerializer;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class KryoSerializerTest {

  private static final int LOOP = 1009;
  private static RssConf rssConf = new RssConf();

  @ParameterizedTest
  @ValueSource(classes = {ByteArrayOutputStream.class, FileOutputStream.class})
  void testKryoWriteRandomRead(Class<?> streamClass, @TempDir File tmpDir) throws Exception{
    boolean isFileMode = streamClass.getName().equals(FileOutputStream.class.getName());
    Kryo kryo = new Kryo();
    OutputStream outputStream = isFileMode ?
        new FileOutputStream(new File(tmpDir, "tmp.kryo")) : new ByteArrayOutputStream();
    Output output = new Output(outputStream);
    long[] offsets = new long[LOOP];
    for (int i = 0; i < LOOP; i++) {
      SomeClass object = (SomeClass) SerializerUtils.genData(SomeClass.class, i);
      kryo.writeObject(output, object);
      offsets[i] = output.total();
    }
    output.close();

    for (int i = 0; i < LOOP; i++) {
      long off = offsets[i];
      Input input = isFileMode ?
          new Input(PartialInputStream.newInputStream(new File(tmpDir, "tmp.kryo"), off, Long.MAX_VALUE)) :
          new Input(PartialInputStream.newInputStream(((ByteArrayOutputStream) outputStream).toByteArray(), off,
              Long.MAX_VALUE));
      for (int j = i + 1; j < LOOP; j++) {
        SomeClass object = kryo.readObject(input, SomeClass.class);
        assertEquals(SerializerUtils.genData(SomeClass.class, j), object);
      }
      input.close();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "java.lang.String,java.lang.Integer,mem",
      "java.lang.String,java.lang.Integer,file",
      "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,int,mem",
      "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,int,file"
  })
  public void testSerDeKeyValues(String classes, @TempDir File tmpDir) throws Exception {
    // 1 Construct serializer
    String[] classArray = classes.split(",");
    Class keyClass = SerializerUtils.getClassByName(classArray[0]);
    Class valueClass = SerializerUtils.getClassByName(classArray[1]);
    boolean isFileMode = classArray[2].equals("file");

    KryoSerializer serializer = new KryoSerializer(rssConf);
    SerializerInstance instance = serializer.newInstance();

    // 2 Write
    OutputStream outputStream = isFileMode ?
        new FileOutputStream(new File(tmpDir, "tmp.kryo")) : new ByteArrayOutputStream();
    SerializationStream serializationStream = instance.serializeStream(outputStream);
    long[] offsets = new long[LOOP];
    for (int i = 0; i < LOOP; i++) {
      serializationStream.writeKey(genData(keyClass, i));
      serializationStream.writeValue(genData(valueClass, i));
      offsets[i] = serializationStream.getTotalBytesWritten();
    }
    serializationStream.close();

    // 3 Random read
    for (int i = 0; i < LOOP; i++) {
      long off = offsets[i];
      PartialInputStream inputStream = isFileMode ?
          PartialInputStream.newInputStream(new File(tmpDir, "tmp.kryo"), off, Long.MAX_VALUE) :
          PartialInputStream.newInputStream(((ByteArrayOutputStream) outputStream).toByteArray(), off, Long.MAX_VALUE);
      DeserializationStream deserializationStream = instance.deserializeStream(inputStream, keyClass, valueClass);
      for (int j = i + 1; j < LOOP; j++) {
        assertEquals(genData(keyClass, j), deserializationStream.readKey());
        assertEquals(genData(valueClass, j), deserializationStream.readValue());
      }
      deserializationStream.close();
    }
  }
}
