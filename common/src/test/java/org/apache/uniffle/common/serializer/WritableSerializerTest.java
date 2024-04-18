package org.apache.uniffle.common.serializer;

import static org.apache.uniffle.common.serializer.SerializerUtils.genData;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.serializer.writable.WritableSerializer;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class WritableSerializerTest {

  private static final int LOOP = 1009;
  private static RssConf rssConf = new RssConf();

  @ParameterizedTest
  @ValueSource(strings = {
      "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,mem",
      "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,file"
  })
  public void testSerDeKeyValues(String classes, @TempDir File tmpDir) throws Exception {
    // 1 Construct serializer
    String[] classArray = classes.split(",");
    Class keyClass = SerializerUtils.getClassByName(classArray[0]);
    Class valueClass = SerializerUtils.getClassByName(classArray[1]);
    boolean isFileMode = classArray[2].equals("file");
    WritableSerializer serializer = new WritableSerializer(rssConf);
    SerializerInstance instance = serializer.newInstance();

    // 2 Write
    OutputStream outputStream = isFileMode ?
        new FileOutputStream(new File(tmpDir, "tmp.data")) : new ByteArrayOutputStream();
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
          PartialInputStream.newInputStream(new File(tmpDir, "tmp.data"), off, Long.MAX_VALUE) :
          PartialInputStream.newInputStream(((ByteArrayOutputStream) outputStream).toByteArray(), off,
              Long.MAX_VALUE);
      DeserializationStream deserializationStream = instance.deserializeStream(inputStream, keyClass, valueClass);
      for (int j = i + 1; j < LOOP; j++) {
        assertEquals(genData(keyClass, j), deserializationStream.readKey());
        assertEquals(genData(valueClass, j), deserializationStream.readValue());
      }
      deserializationStream.close();
    }
  }
}
