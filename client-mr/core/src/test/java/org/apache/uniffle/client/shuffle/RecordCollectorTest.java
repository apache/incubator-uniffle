package org.apache.uniffle.client.shuffle;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class RecordCollectorTest {

  private void reduce(Text key, Iterable<IntWritable> values, RecordCollector collector)
      throws IOException, InterruptedException {
    Iterator<IntWritable> iterator = values.iterator();
    int sum = 0;
    while (iterator.hasNext()) {
      sum += iterator.next().get();
    }
    collector.write(key, new IntWritable(sum));
  }

  @Test
  public void testRecordCollector() throws IOException, InterruptedException {
    RecordBuffer<Text, IntWritable> recordBuffer = new RecordBuffer(-1, true);
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j <= i; j++) {
        Text key = new Text("key" + i);
        recordBuffer.addRecord(key, new IntWritable(j));
      }
    }
    RecordBlob recordBlob = new RecordBlob(-1);
    recordBlob.addRecords(recordBuffer);

    List<Record<Text, IntWritable>> newRecords = new ArrayList<>();
    RecordCollector<Text, IntWritable> collector =
        new RecordCollector(recordBlob.getRecords().entrySet().iterator(), newRecords);
    while (collector.nextKey()) {
      reduce(collector.getCurrentKey(), collector.getValues(), collector);
    }
    assertEquals(5, newRecords.size());
    assertEquals("Record{key=key0, value=0}", newRecords.get(0).toString());
    assertEquals("Record{key=key1, value=1}", newRecords.get(1).toString());
    assertEquals("Record{key=key2, value=3}", newRecords.get(2).toString());
    assertEquals("Record{key=key3, value=6}", newRecords.get(3).toString());
    assertEquals("Record{key=key4, value=10}", newRecords.get(4).toString());
  }
}
