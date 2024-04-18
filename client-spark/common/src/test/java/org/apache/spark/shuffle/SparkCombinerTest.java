package org.apache.spark.shuffle;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Iterator;
import org.apache.spark.Aggregator;
import org.apache.spark.shuffle.SparkCombiner;
import org.apache.uniffle.client.shuffle.Record;
import org.apache.uniffle.client.shuffle.RecordBlob;
import org.apache.uniffle.client.shuffle.RecordBuffer;
import org.junit.jupiter.api.Test;

public class SparkCombinerTest {

  @Test
  public void testSparkCombiner() {
    RecordBuffer<String, Integer> recordBuffer = new RecordBuffer(-1);
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j <= i; j++) {
        String key = "key" + i;
        recordBuffer.addRecord(key, j);
      }
    }
    SparkCombiner<String, Integer, String> combiner = new SparkCombiner<>(new Aggregator<>(
        (v) -> v.toString(),
        (c, v) -> Integer.valueOf(Integer.parseInt(c) + v).toString(),
        (c1, c2) -> Integer.valueOf(Integer.parseInt(c1) + Integer.parseInt(c2)).toString()
    ));
    RecordBlob recordBlob = new RecordBlob(-1);
    recordBlob.addRecords(recordBuffer);
    recordBlob.combine(combiner, false);
    Iterator<Record<String, Integer>> newRecords = recordBlob.getResult().iterator();
    int index = 0;
    while(newRecords.hasNext()) {
      Record<String, Integer> record = newRecords.next();
      int expectedValue = 0;
      for (int i = 0; i <= index; i++) {
        expectedValue += i;
      }
      assertEquals("Record{key=key"+ index +", value=" + expectedValue + "}", record.toString());
      index++;
    }
    assertEquals(5, index);
  }
}
