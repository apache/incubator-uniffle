package org.apache.uniffle.client.shuffle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.uniffle.client.shuffle.writer.Combiner;
import org.apache.uniffle.common.exception.RssException;

public class MRCombiner<K extends Writable, V extends Writable> extends Combiner<K, V, V> {

  private JobConf jobConf;
  private Class combineClass;

  public MRCombiner(JobConf jobConf, Class combineClass) {
    this.jobConf = jobConf;
    this.combineClass = combineClass;
  }

  @Override
  public List<Record<K, V>> combineValues(Iterator<Map.Entry<K, List<Record<K, V>>>> recordIterator) {
    try {
      // TODO: For now, only MapReduce new api is supported.
      List<Record<K, V>> ret = new ArrayList<>();
      Reducer<K, V, K, V> reducer = (Reducer) ReflectionUtils.newInstance(this.combineClass, this.jobConf);
      Reducer.Context context = new WrappedReducer<K, V, K, V>().getReducerContext(
          new RecordCollector(recordIterator, ret));
      reducer.run(context);
      return ret;
    } catch (IOException | InterruptedException e) {
      throw new RssException(e);
    }
  }

  @Override
  public List<Record<K, V>> combineCombiners(Iterator<Map.Entry<K, List<Record<K, V>>>> recordIterator) {
    return combineValues(recordIterator);
  }
}
