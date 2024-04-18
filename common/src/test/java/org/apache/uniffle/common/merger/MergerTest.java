package org.apache.uniffle.common.merger;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.records.RecordsReader;
import org.apache.uniffle.common.serializer.PartialInputStream;
import org.apache.uniffle.common.serializer.SerializerUtils;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class MergerTest {

  private final static int RECORDS = 1009;
  private final static int SEGMENTS = 4;

  @ParameterizedTest
  @ValueSource(strings = {
      "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable",
      "java.lang.String,java.lang.Integer",
      "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer",
  })
  public void testMergeSegmentToFile(String classes, @TempDir File tmpDir) throws Exception {
    // 1 Parse arguments
    String[] classArray = classes.split(",");
    Class keyClass = SerializerUtils.getClassByName(classArray[0]);
    Class valueClass = SerializerUtils.getClassByName(classArray[1]);

    // 2 Construct segments, then merge
    RssConf rssConf = new RssConf();
    List<AbstractSegment> segments = new ArrayList<>();
    Comparator comparator = SerializerUtils.getComparator(keyClass);
    for (int i = 0; i < SEGMENTS; i++) {
      if (i % 2 == 0) {
        segments.add(SerializerUtils.genMemorySegment(rssConf, keyClass, valueClass, i, i, SEGMENTS, RECORDS));
      } else {
        segments.add(SerializerUtils.genFileSegment(rssConf, keyClass, valueClass, i, i, SEGMENTS, RECORDS, tmpDir));
      }
    }
    File mergedFile = new File(tmpDir, "data.merged");
    FileOutputStream outputStream = new FileOutputStream(mergedFile);
    Merger.merge(rssConf, outputStream, segments, keyClass,  valueClass, comparator);
    outputStream.close();

    // 3 Check the merged file
    RecordsReader reader = new RecordsReader(rssConf,
        PartialInputStream.newInputStream(mergedFile, 0, mergedFile.length()), keyClass, valueClass);
    int index = 0;
    while (reader.hasNext()) {
      reader.next();
      assertEquals(SerializerUtils.genData(keyClass, index), reader.getCurrentKey());
      assertEquals(SerializerUtils.genData(valueClass, index), reader.getCurrentValue());
      index++;
    }
    assertEquals(RECORDS * SEGMENTS, index);
    reader.close();
  }
}
