package org.apache.spark.shuffle.writer;

import static org.apache.spark.shuffle.RssSparkConfig.RSS_MERGED_WRITE_MAX_RECORDS;
import static org.apache.spark.shuffle.RssSparkConfig.RSS_MERGED_WRITE_MAX_RECORDS_PER_BUFFER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.Aggregator;
import org.apache.spark.SparkConf;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.shuffle.RssSparkConfig;
import org.apache.spark.shuffle.SparkCombiner;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.records.RecordsReader;
import org.apache.uniffle.common.serializer.PartialInputStreamImpl;
import org.apache.uniffle.common.serializer.SerializerUtils;
import org.apache.uniffle.common.util.JavaUtils;
import org.junit.jupiter.api.Test;
import scala.math.Ordering;

public class RMWriteBufferManagerTest {

  private final static int RECORDS = 1009;

  @Test
  public void testWriteNormal() throws Exception {
    SparkConf conf = new SparkConf();
    RssConf rssConf = RssSparkConfig.toRssConf(conf);
    BufferManagerOptions bufferOptions = new BufferManagerOptions(conf);
    TaskMemoryManager taskMemoryManager = mock(TaskMemoryManager.class);
    RMWriteBufferManager manager =
        new RMWriteBufferManager(
            0,
            null,
            0,
            bufferOptions,
            JavaUtils.newConcurrentMap(),
            taskMemoryManager,
            rssConf,
            Ordering.String$.MODULE$,
            null,
            String.class,
            Integer.class,
            null,
            false,
            RSS_MERGED_WRITE_MAX_RECORDS_PER_BUFFER.defaultValue().get(),
            RSS_MERGED_WRITE_MAX_RECORDS.defaultValue().get(),
            null);
    List<ShuffleBlockInfo> shuffleBlockInfos;
    List<Integer> indexes = new ArrayList<>();
    for (int i = 0; i < RECORDS; i++) {
      indexes.add(i);
    }
    Collections.shuffle(indexes);
    for (Integer index : indexes) {
      shuffleBlockInfos = manager.addRecord(0, SerializerUtils.genData(String.class, index),
          SerializerUtils.genData(Integer.class, index));
      assertTrue(CollectionUtils.isEmpty(shuffleBlockInfos));
    }

    shuffleBlockInfos = manager.clear();
    assertFalse(CollectionUtils.isEmpty(shuffleBlockInfos));

    // check blocks
    List<AddBlockEvent> events = manager.buildBlockEvents(shuffleBlockInfos);
    assertEquals(1, events.size());
    List<ShuffleBlockInfo> blocks = events.get(0).getShuffleDataInfoList();
    assertEquals(1, blocks.size());

    ByteBuf buf = blocks.get(0).getData();
    byte[] bytes = new byte[blocks.get(0).getLength()];
    buf.readBytes(bytes);

    RecordsReader<String, Integer> reader = new RecordsReader<>(rssConf,
        PartialInputStreamImpl.newInputStream(bytes, 0, bytes.length), String.class, Integer.class);
    int index = 0;
    while (reader.hasNext()) {
      reader.next();
      assertEquals(SerializerUtils.genData(String.class, index), reader.getCurrentKey());
      assertEquals(SerializerUtils.genData(Integer.class, index), reader.getCurrentValue());
      index++;
    }
    reader.close();
    assertEquals(RECORDS, index);
  }

  @Test
  public void testWriteNormalWithCombine() throws Exception {
    SparkConf conf = new SparkConf();
    RssConf rssConf = RssSparkConfig.toRssConf(conf);
    BufferManagerOptions bufferOptions = new BufferManagerOptions(conf);
    SparkCombiner<String, Integer, String> combiner = new SparkCombiner<>(new Aggregator<>(
        (v) -> v.toString(),
        (c, v) -> Integer.valueOf(Integer.parseInt(c) + v).toString(),
        (c1, c2) -> Integer.valueOf(Integer.parseInt(c1) + Integer.parseInt(c2)).toString()
    ));
    TaskMemoryManager taskMemoryManager = mock(TaskMemoryManager.class);
    RMWriteBufferManager manager =
        new RMWriteBufferManager(
            0,
            null,
            0,
            bufferOptions,
            JavaUtils.newConcurrentMap(),
            taskMemoryManager,
            rssConf,
            Ordering.String$.MODULE$,
            combiner,
            String.class,
            Integer.class,
            String.class,
            true,
            RSS_MERGED_WRITE_MAX_RECORDS_PER_BUFFER.defaultValue().get(),
            RSS_MERGED_WRITE_MAX_RECORDS.defaultValue().get(),
            null);
    List<ShuffleBlockInfo> shuffleBlockInfos;
    List<Integer> indexes = new ArrayList<>();
    for (int i = 0; i < RECORDS; i++) {
      indexes.add(i);
    }
    Collections.shuffle(indexes);
    for (Integer index : indexes) {
      int times = index % 3 + 1;
      for (int j = 0; j < times; j++) {
        shuffleBlockInfos = manager.addRecord(0, SerializerUtils.genData(String.class, index),
            SerializerUtils.genData(Integer.class, index + j));
        assertTrue(CollectionUtils.isEmpty(shuffleBlockInfos));
      }
    }
    shuffleBlockInfos = manager.clear();
    assertFalse(CollectionUtils.isEmpty(shuffleBlockInfos));

    // check blocks
    List<AddBlockEvent> events = manager.buildBlockEvents(shuffleBlockInfos);
    assertEquals(1, events.size());
    List<ShuffleBlockInfo> blocks = events.get(0).getShuffleDataInfoList();
    assertEquals(1, blocks.size());

    ByteBuf buf = blocks.get(0).getData();
    byte[] bytes = new byte[blocks.get(0).getLength()];
    buf.readBytes(bytes);

    RecordsReader<String, String> reader = new RecordsReader<>(rssConf,
        PartialInputStreamImpl.newInputStream(bytes, 0, bytes.length), String.class, String.class);
    int index = 0;
    while (reader.hasNext()) {
      reader.next();
      int aimValue = index;
      if (index % 3 == 1) {
        aimValue = 2 * aimValue + 1;
      }
      if (index % 3 == 2) {
        aimValue = 3 * aimValue + 3;
      }
      assertEquals(SerializerUtils.genData(String.class, index), reader.getCurrentKey());
      assertEquals(SerializerUtils.genData(Integer.class, aimValue), Integer.parseInt(reader.getCurrentValue()));
      index++;
    }
    reader.close();
    assertEquals(RECORDS, index);
  }
}
