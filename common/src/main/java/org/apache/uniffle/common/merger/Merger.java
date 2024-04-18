package org.apache.uniffle.common.merger;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import org.apache.hadoop.util.PriorityQueue;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.records.RecordsWriter;

public class Merger<K, V> {

  public static class MergeQueue<K, V> extends PriorityQueue<AbstractSegment<K, V>> implements KeyValueIterator<K, V> {

    private final RssConf rssConf;
    private final List<AbstractSegment<K, V>> segments;
    private Class<K> keyClass;
    private Class<V> valueClass;
    private Comparator comparator;

    private K currentKey;
    private V currentValue;
    AbstractSegment<K, V> minSegment;
    Function<Integer, AbstractSegment> popSegmentHook;

    public MergeQueue(RssConf rssConf, List<AbstractSegment<K, V>> segments, Class<K> keyClass, Class<V> valueClass,
                      Comparator<K> comparator) {
      this.rssConf = rssConf;
      this.segments = segments;
      this.keyClass = keyClass;
      this.valueClass = valueClass;
      if (comparator == null) {
        throw new RssException("comparator is null!");
      } else {
        this.comparator = comparator;
      }
    }

    public void setPopSegmentHook(Function<Integer, AbstractSegment> popSegmentHook) {
      this.popSegmentHook = popSegmentHook;
    }

    @Override
    protected boolean lessThan(Object o1, Object o2) {
      AbstractSegment s1 = (AbstractSegment) o1;
      AbstractSegment s2 = (AbstractSegment) o2;
      Object key1 = s1.getCurrentKey();
      Object key2 = s2.getCurrentKey();
      int c = comparator.compare(key1, key2);
      return c < 0 || ((c == 0) && s1.getId() < s2.getId());
    }

    public void init() throws IOException {
      List<AbstractSegment<K, V>> segmentsToMerge = new ArrayList();
      for (AbstractSegment<K, V> segment : segments) {
        boolean hasNext = segment.hasNext();
        if (hasNext) {
          segment.next();
          segmentsToMerge.add(segment);
        } else {
          segment.close();
        }
      }
      initialize(segmentsToMerge.size());
      clear();
      for (AbstractSegment segment : segmentsToMerge) {
        put(segment);
      }
    }

    @Override
    public K getCurrentKey() {
      return currentKey;
    }

    @Override
    public V getCurrentValue() {
      return currentValue;
    }

    @Override
    public boolean next() throws IOException {
      if (size() == 0) {
        resetKeyValue();
        return false;
      }

      if (minSegment != null) {
        adjustPriorityQueue(minSegment);
        if (size() == 0) {
          minSegment = null;
          resetKeyValue();
          return false;
        }
      }
      minSegment = top();
      currentKey = minSegment.getCurrentKey();
      currentValue = minSegment.getCurrentValue();
      return true;
    }

    private void resetKeyValue() {
      currentKey = null;
      currentValue = null;
    }

    private void adjustPriorityQueue(AbstractSegment segment) throws IOException{
      if (segment.hasNext()) {
        segment.next();
        adjustTop();
      } else {
        pop();
        segment.close();
        if (popSegmentHook != null) {
          AbstractSegment newSegment = popSegmentHook.apply((int)segment.getId());
          if (newSegment != null) {
            if (newSegment.hasNext()) {
              newSegment.next();
              put(newSegment);
            } else {
              newSegment.close();
            }
          }
        }
      }
    }

    void merge(OutputStream output) throws IOException {
      RecordsWriter<K, V> writer = new RecordsWriter<K, V>(rssConf, output, keyClass, valueClass);
      boolean recorded = true;
      while(this.next()) {
        writer.append(this.getCurrentKey(), this.getCurrentValue());
        if (output instanceof Recordable) {
          recorded = ((Recordable) output).record(writer.getTotalBytesWritten(), () -> writer.flush(), false);
        }
      }
      writer.flush();
      if (!recorded) {
        ((Recordable) output).record(writer.getTotalBytesWritten(), null, true);
      }
      writer.close();
    }

    @Override
    public void close() throws IOException {
      AbstractSegment segment;
      while((segment = pop()) != null) {
        segment.close();
      }
    }
  }

  public static void merge(RssConf conf, OutputStream output, List<AbstractSegment> segments, Class keyClass,
                           Class valueClass, Comparator comparator) throws IOException {
    MergeQueue mergeQueue = new MergeQueue(conf, segments, keyClass, valueClass, comparator);
    mergeQueue.init();
    mergeQueue.merge(output);
    mergeQueue.close();
  }
}
