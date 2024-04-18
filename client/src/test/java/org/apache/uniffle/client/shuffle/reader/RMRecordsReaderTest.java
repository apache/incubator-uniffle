package org.apache.uniffle.client.shuffle.reader;

import static org.apache.uniffle.common.serializer.SerializerUtils.genSortedRecordBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.uniffle.client.api.ShuffleServerClient;
import org.apache.uniffle.client.shuffle.writer.Combiner;
import org.apache.uniffle.client.shuffle.writer.SumByKeyCombiner;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.merger.AbstractSegment;
import org.apache.uniffle.common.merger.Merger;
import org.apache.uniffle.common.serializer.SerializerUtils;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class RMRecordsReaderTest {

  private final static String APP_ID = "app1";
  private final static int SHUFFLE_ID = 0;
  private final static int RECORDS_NUM = 1009;

  @Timeout(10)
  @ParameterizedTest
  @ValueSource(strings = {
      "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable",
      "java.lang.String,java.lang.Integer",
      "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer",
  })
  public void testNormalReadWithoutCombine(String classes) throws Exception {
    // 1 basic parameter
    final String[] classArray = classes.split(",");
    final String keyClassName = classArray[0];
    final String valueClassName = classArray[1];
    final Class keyClass = SerializerUtils.getClassByName(keyClassName);
    final Class valueClass = SerializerUtils.getClassByName(valueClassName);
    final Comparator comparator = SerializerUtils.getComparator(keyClass);
    final Combiner combiner = null;
    final int partitionId = 0;
    final RssConf rssConf = new RssConf();
    final List<ShuffleServerInfo> serverInfos = new ArrayList<>();
    serverInfos.add(new ShuffleServerInfo("dummy", -1));

    // 2 construct reader
    RMRecordsReader reader = new RMRecordsReader(APP_ID, SHUFFLE_ID, Sets.newHashSet(partitionId),
        ImmutableMap.of(partitionId, serverInfos), rssConf, keyClass, valueClass, comparator, combiner, false, null);
    RMRecordsReader readerSpy = spy(reader);
    byte[] buffers = genSortedRecordBytes(rssConf, keyClass, valueClass, 0, 1, RECORDS_NUM, 1);
    ShuffleServerClient serverClient = new MockedShuffleServerClient(new int[] {partitionId},
        new ByteBuffer[][] {{ByteBuffer.wrap(buffers)}}, null);
    doReturn(serverClient).when(readerSpy).createShuffleServerClient(any());

    // 3 run reader and verify result
    readerSpy.start();
    int index = 0;
    KeyValueReader keyValueReader = readerSpy.keyValueReader();
    while (keyValueReader.next()) {
      assertEquals(SerializerUtils.genData(keyClass, index), keyValueReader.getCurrentKey());
      assertEquals(SerializerUtils.genData(valueClass, index), keyValueReader.getCurrentValue());
      index++;
    }
    assertEquals(RECORDS_NUM, index);
  }

  @Timeout(10)
  @ParameterizedTest
  @ValueSource(strings = {
      "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable",
      "java.lang.String,java.lang.Integer",
      "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer",
  })
  public void testNormalReadWithCombine(String classes) throws Exception {
    // 1 basic parameter
    final String[] classArray = classes.split(",");
    final String keyClassName = classArray[0];
    final String valueClassName = classArray[1];
    final Class keyClass = SerializerUtils.getClassByName(keyClassName);
    final Class valueClass = SerializerUtils.getClassByName(valueClassName);
    final Comparator comparator = SerializerUtils.getComparator(keyClass);
    final Combiner combiner = new SumByKeyCombiner();
    final int partitionId = 0;
    final RssConf rssConf = new RssConf();
    final List<ShuffleServerInfo> serverInfos = new ArrayList<>();
    serverInfos.add(new ShuffleServerInfo("dummy", -1));

    // 2 construct reader
    RMRecordsReader reader = new RMRecordsReader(APP_ID, SHUFFLE_ID, Sets.newHashSet(partitionId),
        ImmutableMap.of(partitionId, serverInfos), rssConf, keyClass, valueClass, comparator, combiner, false, null);
    RMRecordsReader readerSpy = spy(reader);
    List<AbstractSegment> segments = new ArrayList<>();
    segments.add(SerializerUtils.genMemorySegment(rssConf, keyClass, valueClass, 0L, 0, 2, RECORDS_NUM));
    segments.add(SerializerUtils.genMemorySegment(rssConf, keyClass, valueClass, 1L, 0, 2, RECORDS_NUM));
    segments.add(SerializerUtils.genMemorySegment(rssConf, keyClass, valueClass, 2L, 1, 2, RECORDS_NUM));
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    Merger.merge(rssConf, output, segments, keyClass, valueClass, comparator);
    output.close();
    byte[] buffers = output.toByteArray();
    ShuffleServerClient serverClient = new MockedShuffleServerClient(new int[] {partitionId},
        new ByteBuffer[][] {{ByteBuffer.wrap(buffers)}}, null);
    doReturn(serverClient).when(readerSpy).createShuffleServerClient(any());

    // 3 run reader and verify result
    readerSpy.start();
    int index = 0;
    KeyValueReader keyValueReader = readerSpy.keyValueReader();
    while (keyValueReader.next()) {
      assertEquals(SerializerUtils.genData(keyClass, index), keyValueReader.getCurrentKey());
      Object value = SerializerUtils.genData(valueClass, index);
      Object newValue = value;
      if (index % 2 == 0) {
        if (value instanceof IntWritable) {
          newValue = new IntWritable(((IntWritable) value).get() * 2);
        } else {
          newValue = (int) value * 2;
        }
      }
      assertEquals(newValue, keyValueReader.getCurrentValue());
      index++;
    }
    assertEquals(RECORDS_NUM * 2, index);
  }

  @Timeout(10)
  @ParameterizedTest
  @ValueSource(strings = {
      "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable",
      "java.lang.String,java.lang.Integer",
      "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer",
  })
  public void testReadMulitPartitionWithoutCombine(String classes) throws Exception {
    // 1 basic parameter
    final String[] classArray = classes.split(",");
    final String keyClassName = classArray[0];
    final String valueClassName = classArray[1];
    final Class keyClass = SerializerUtils.getClassByName(keyClassName);
    final Class valueClass = SerializerUtils.getClassByName(valueClassName);
    final Comparator comparator = SerializerUtils.getComparator(keyClass);
    final Combiner combiner = null;
    final int partitionId = 0;
    final RssConf rssConf = new RssConf();
    final List<ShuffleServerInfo> serverInfos = new ArrayList<>();
    serverInfos.add(new ShuffleServerInfo("dummy", -1));

    // 2 construct reader
    RMRecordsReader reader = new RMRecordsReader(APP_ID, SHUFFLE_ID,
        Sets.newHashSet(partitionId, partitionId + 1, partitionId + 2),
        ImmutableMap.of(partitionId, serverInfos, partitionId + 1, serverInfos, partitionId + 2, serverInfos),
        rssConf,
        keyClass,
        valueClass,
        comparator,
        combiner,
        false,
        null);
    RMRecordsReader readerSpy = spy(reader);
    ByteBuffer[][] buffers = new ByteBuffer[3][2];
    for (int i= 0; i < 3; i ++) {
      buffers[i][0] = ByteBuffer.wrap(genSortedRecordBytes(rssConf, keyClass, valueClass, i, 3, RECORDS_NUM, 1));
      buffers[i][1] =
          ByteBuffer.wrap(genSortedRecordBytes(rssConf, keyClass, valueClass, i + RECORDS_NUM * 3, 3, RECORDS_NUM, 1));
    }
    ShuffleServerClient serverClient = new MockedShuffleServerClient(
        new int[] {partitionId, partitionId + 1, partitionId + 2}, buffers, null);
    doReturn(serverClient).when(readerSpy).createShuffleServerClient(any());

    // 3 run reader and verify result
    readerSpy.start();
    int index = 0;
    KeyValueReader keyValueReader = readerSpy.keyValueReader();
    while (keyValueReader.next()) {
      assertEquals(SerializerUtils.genData(keyClass, index), keyValueReader.getCurrentKey());
      assertEquals(SerializerUtils.genData(valueClass, index), keyValueReader.getCurrentValue());
      index++;
    }
    assertEquals(RECORDS_NUM * 6, index);
  }

  //@Timeout(10)
  @ParameterizedTest
  @ValueSource(strings = {
      "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable",
      "java.lang.String,java.lang.Integer",
      "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer",
  })
  public void testReadMulitPartitionWithCombine(String classes) throws Exception {
    // 1 basic parameter
    final String[] classArray = classes.split(",");
    final String keyClassName = classArray[0];
    final String valueClassName = classArray[1];
    final Class keyClass = SerializerUtils.getClassByName(keyClassName);
    final Class valueClass = SerializerUtils.getClassByName(valueClassName);
    final Comparator comparator = SerializerUtils.getComparator(keyClass);
    final Combiner combiner = new SumByKeyCombiner();
    final int partitionId = 0;
    final RssConf rssConf = new RssConf();
    final List<ShuffleServerInfo> serverInfos = new ArrayList<>();
    serverInfos.add(new ShuffleServerInfo("dummy", -1));

    // 2 construct reader
    RMRecordsReader reader = new RMRecordsReader(APP_ID, SHUFFLE_ID,
        Sets.newHashSet(partitionId, partitionId + 1, partitionId + 2),
        ImmutableMap.of(partitionId, serverInfos, partitionId + 1, serverInfos, partitionId + 2, serverInfos),
        rssConf,
        keyClass,
        valueClass,
        comparator,
        combiner,
        false,
        null);
    RMRecordsReader readerSpy = spy(reader);
    ByteBuffer[][] buffers = new ByteBuffer[3][2];
    for (int i= 0; i < 3; i ++) {
      buffers[i][0] = ByteBuffer.wrap(genSortedRecordBytes(rssConf, keyClass, valueClass, i, 3, RECORDS_NUM, 2));
      buffers[i][1] =
          ByteBuffer.wrap(genSortedRecordBytes(rssConf, keyClass, valueClass, i + RECORDS_NUM * 3, 3, RECORDS_NUM, 2));
    }
    ShuffleServerClient serverClient = new MockedShuffleServerClient(
        new int[] {partitionId, partitionId + 1, partitionId + 2}, buffers, null);
    doReturn(serverClient).when(readerSpy).createShuffleServerClient(any());

    // 3 run reader and verify result
    readerSpy.start();
    int index = 0;
    KeyValueReader keyValueReader = readerSpy.keyValueReader();
    while (keyValueReader.next()) {
      assertEquals(SerializerUtils.genData(keyClass, index), keyValueReader.getCurrentKey());
      assertEquals(SerializerUtils.genData(valueClass, index * 2), keyValueReader.getCurrentValue());
      index++;
    }
    assertEquals(RECORDS_NUM * 6, index);
  }
}
