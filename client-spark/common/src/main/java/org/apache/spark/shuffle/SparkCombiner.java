package org.apache.spark.shuffle;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.spark.Aggregator;
import org.apache.uniffle.client.shuffle.writer.Combiner;
import org.apache.uniffle.client.shuffle.Record;

public class SparkCombiner<K, V, C> extends Combiner<K, V, C> {

  private Aggregator<K, V, C> agg;

  public SparkCombiner(Aggregator<K, V, C> agg) {
    this.agg = agg;
  }

  @Override
  public List<Record<K, C>> combineValues(Iterator<Map.Entry<K, List<Record<K, V>>>> recordIterator) {
    List<Record<K, C>> ret = new ArrayList<>();
    while(recordIterator.hasNext()) {
      Map.Entry<K, List<Record<K, V>>> entry = recordIterator.next();
      List<Record<K, V>> records = entry.getValue();
      Record<K, C> current = null;
      for (Record<K, V> record : records) {
        if (current == null) {
          // Handle new Key
          current = Record.create(record.getKey(), agg.createCombiner().apply(record.getValue()));
        } else {
          // Combine the values
          C newValue = agg.mergeValue().apply(current.getValue(), record.getValue());
          current.setValue(newValue);
        }
      }
      ret.add(current);
    }
    return ret;
  }

  @Override
  public List<Record<K, C>> combineCombiners(Iterator<Map.Entry<K, List<Record<K, C>>>> recordIterator) {
    List<Record<K, C>> ret = new ArrayList<>();
    while(recordIterator.hasNext()) {
      Map.Entry<K, List<Record<K, C>>> entry = recordIterator.next();
      List<Record<K, C>> records = entry.getValue();
      Record<K, C> current = null;
      for (Record<K, C> record : records) {
        if (current == null) {
          // Handle new Key
          current = record;
        } else {
          // Combine the values
          C newValue = agg.mergeCombiners().apply(current.getValue(), record.getValue());
          current.setValue(newValue);
        }
      }
      ret.add(current);
    }
    return ret;
  }
}
