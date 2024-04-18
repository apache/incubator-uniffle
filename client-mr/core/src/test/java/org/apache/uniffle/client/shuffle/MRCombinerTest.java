package org.apache.uniffle.client.shuffle;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class MRCombinerTest {

  public static class IntSumReducer
      extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  @Test
  @Timeout(5)
  public void testMRCombiner() {
    RecordBuffer recordBuffer = new RecordBuffer(-1, true);
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j <= i; j++) {
        Text key = new Text("key" + i);
        recordBuffer.addRecord(key, new IntWritable(j));
      }
    }
    JobConf jobConf = new JobConf();
    MRCombiner<Text, IntWritable> combiner = new MRCombiner<>(jobConf, IntSumReducer.class);
    RecordBlob recordBlob = new RecordBlob(-1);
    recordBlob.addRecords(recordBuffer);
    recordBlob.combine(combiner, false);
    Iterator<Record<Text, IntWritable>> newRecords = recordBlob.getResult().iterator();
    int index = 0;
    while(newRecords.hasNext()) {
      Record<Text, IntWritable> record = newRecords.next();
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
