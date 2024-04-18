package org.apache.uniffle.common.records;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Random;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.serializer.PartialInputStream;
import org.apache.uniffle.common.serializer.SerializerUtils;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class RecordsReaderWriterTest {

  private final static int RECORDS = 1009;
  private final static int LOOP = 5;

  @ParameterizedTest
  @ValueSource(strings = {
      "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,mem",
      "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,file",
      "java.lang.String,java.lang.Integer,mem",
      "java.lang.String,java.lang.Integer,file",
      "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer,mem",
      "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer,file"
  })
  public void testWriteAndReadRecordFile(String classes, @TempDir File tmpDir) throws Exception {
    RssConf rssConf = new RssConf();
    // 1 Parse arguments
    String[] classArray = classes.split(",");
    Class keyClass = SerializerUtils.getClassByName(classArray[0]);
    Class valueClass = SerializerUtils.getClassByName(classArray[1]);
    boolean isFileMode = classArray[2].equals("file");
    File tmpFile = new File(tmpDir, "tmp.data");

    // 2 Write
    long[] offsets = new long[RECORDS];
    OutputStream outputStream = isFileMode ? new FileOutputStream(tmpFile) : new ByteArrayOutputStream();
    RecordsWriter writer = new RecordsWriter(rssConf, outputStream, keyClass, valueClass);
    for (int i = 0; i < RECORDS; i++) {
      writer.append(SerializerUtils.genData(keyClass, i), SerializerUtils.genData(valueClass, i));
      offsets[i] = writer.getTotalBytesWritten();
    }
    writer.close();

    // 3 Read
    // 3.1 read from start
    PartialInputStream inputStream = isFileMode ?
        PartialInputStream.newInputStream(tmpFile, 0, tmpFile.length()) :
        PartialInputStream.newInputStream(((ByteArrayOutputStream) outputStream).toByteArray(), 0,
            Long.MAX_VALUE);
    RecordsReader reader = new RecordsReader(rssConf, inputStream, keyClass, valueClass);
    int index = 0;
    while (reader.hasNext()) {
      reader.next();
      assertEquals(SerializerUtils.genData(keyClass, index), reader.getCurrentKey());
      assertEquals(SerializerUtils.genData(valueClass, index), reader.getCurrentValue());
      index ++;
    }
    assertEquals(RECORDS, index);
    reader.close();

    // 3.2 read from end
    inputStream = isFileMode ? PartialInputStream.newInputStream(tmpFile, tmpFile.length(), tmpFile.length()) :
        PartialInputStream.newInputStream(((ByteArrayOutputStream) outputStream).toByteArray(),
            ((ByteArrayOutputStream) outputStream).size(), Long.MAX_VALUE);
    reader = new RecordsReader(rssConf, inputStream, keyClass, valueClass);
    assertFalse(reader.hasNext());
    reader.close();

    // 2.4 read from random position to end
    Random random = new Random();
    long [][] indexAndOffsets = new long[LOOP + 3][2];
    indexAndOffsets[0] = new long[]{0, 0};
    indexAndOffsets[1] = new long[]{RECORDS - 1, offsets[RECORDS - 2]};      // Last record
    indexAndOffsets[2] = new long[]{RECORDS, offsets[RECORDS - 1]};      // Records that don't exist
    for (int i = 0; i < LOOP; i++) {
      int off = random.nextInt(RECORDS - 2) + 1;
      indexAndOffsets[i + 3] = new long[] {off + 1, offsets[off]};
    }
    for (long[] indexAndOffset : indexAndOffsets) {
      index = (int) indexAndOffset[0];
      long offset = indexAndOffset[1];
      inputStream = isFileMode ? PartialInputStream.newInputStream(tmpFile, offset, tmpFile.length()) :
          PartialInputStream.newInputStream(((ByteArrayOutputStream) outputStream).toByteArray(), offset,
              Long.MAX_VALUE);
      reader = new RecordsReader(rssConf, inputStream, keyClass, valueClass);
      while(reader.hasNext()) {
        reader.next();
        assertEquals(SerializerUtils.genData(keyClass, index), reader.getCurrentKey());
        assertEquals(SerializerUtils.genData(valueClass, index), reader.getCurrentValue());
        index ++;
      }
      assertEquals(RECORDS, index);
    }
    reader.close();
  }
}
