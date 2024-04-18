package org.apache.uniffle.client.shuffle.writer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.uniffle.client.shuffle.Record;

public class SumByKeyCombiner extends Combiner {

  @Override
  public List<Record> combineValues(Iterator recordIterator) {
    List<Record> ret = new ArrayList<>();
    while (recordIterator.hasNext()) {
      Map.Entry<Object, List<Record>> entry = (Map.Entry<Object, List<Record>>) recordIterator.next();
      Record current = null;
      List<Record> records = entry.getValue();
      for (Record record: records) {
        if (current == null) {
          current = record;
          ret.add(current);
        } else {
          int v1 = record.getValue() instanceof IntWritable ? ((IntWritable) record.getValue()).get() :
              (int) record.getValue();
          int v2 = current.getValue() instanceof IntWritable ? ((IntWritable) current.getValue()).get() :
              (int) current.getValue();
          if (current.getValue() instanceof IntWritable) {
            ((IntWritable) current.getValue()).set(v1 + v2);
          } else {
            current.setValue(v1 + v2);
          }
        }
      }
    }
    return ret;
  }

  @Override
  public List<Record> combineCombiners(Iterator recordIterator) {
    return combineValues(recordIterator);
  }
}
