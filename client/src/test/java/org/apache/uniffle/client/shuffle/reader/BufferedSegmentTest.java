package org.apache.uniffle.client.shuffle.reader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.uniffle.client.shuffle.RecordBuffer;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.merger.AbstractSegment;
import org.apache.uniffle.common.merger.Merger;
import org.apache.uniffle.common.serializer.SerializerUtils;
import org.junit.jupiter.api.Test;

public class BufferedSegmentTest {

  private final static int RECORDS = 1000;
  private final static int SEGMENTS = 4;

  @Test
  public void testMergeResolvedSegmentWithHook() throws Exception {
    RssConf rssConf = new RssConf();
    List<AbstractSegment> segments = new ArrayList<>();
    Comparator comparator = new Text.Comparator();
    for (int i = 0; i < SEGMENTS; i++) {
      if (i % 2 == 0) {
        segments.add(genResolvedSegment(Text.class, IntWritable.class, i, i, 4, RECORDS / 2));
      } else {
        segments.add(genResolvedSegment(Text.class, IntWritable.class, i, i, 4, RECORDS));
      }
    }
    Map<Integer, AbstractSegment> newSegments = new HashMap<>();
    for (int i = 0; i < SEGMENTS; i++) {
      if (i % 2 == 0) {
        newSegments.put(i, genResolvedSegment(Text.class, IntWritable.class, i, i + RECORDS * 2, 4, RECORDS / 2));
      } else {
        newSegments.put(i, genResolvedSegment(Text.class, IntWritable.class, i, i + RECORDS * 4, 4, RECORDS));
      }
    }
    Merger.MergeQueue mergeQueue = new Merger.MergeQueue(rssConf, segments, Text.class, IntWritable.class, comparator);
    mergeQueue.init();
    mergeQueue.setPopSegmentHook(id -> {
      AbstractSegment newSegment = newSegments.get(id);
      if (newSegment != null) {
        newSegments.remove(id);
      }
      return newSegment;
    });
    for (int i = 0; i < RECORDS * 8; i++) {
      if ((i >= 4 * RECORDS) && (i % 2 == 0)) {
        continue;
      }
      mergeQueue.next();
      assertEquals(SerializerUtils.genData(Text.class, i), mergeQueue.getCurrentKey());
      assertEquals(SerializerUtils.genData(IntWritable.class, i), mergeQueue.getCurrentValue());
    }
    assertFalse(mergeQueue.next());
  }

  private static BufferedSegment genResolvedSegment(Class keyClass, Class valueClass, int pid, int start, int interval,
                                                    int length) {
    RecordBuffer buffer = new RecordBuffer(pid);
    for (int i = 0; i < length; i++) {
      buffer.addRecord(SerializerUtils.genData(keyClass, start + i * interval),
          SerializerUtils.genData(valueClass, start + i * interval));
    }
    return new BufferedSegment(buffer);
  }
}
