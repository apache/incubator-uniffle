package org.apache.uniffle.client.shuffle.writer;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.uniffle.client.shuffle.Record;

/*
 * If shuffle requires sorting, the input records are sorted by key, so they can be combined sequentially.
 * If shuffle does not require sorting. The input records are sorted according to the hashcode of the key.
 * Therefore, the same keys may not be organized together, so the data cannot be obtained sequentially.
 * So LinkedHashMap needs to be used.
 * */
public abstract class Combiner<K, V, C> {

  public abstract List<Record<K, C>> combineValues(Iterator<Map.Entry<K, List<Record<K, V>>>> recordIterator);

  public abstract List<Record<K, C>> combineCombiners(Iterator<Map.Entry<K, List<Record<K, C>>>> recordIterator);
}
