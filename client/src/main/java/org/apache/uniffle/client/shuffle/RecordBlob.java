package org.apache.uniffle.client.shuffle;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.uniffle.client.shuffle.writer.Combiner;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.records.RecordsWriter;

/*
 * RecordBlob is used to store records. The records are stored in the form
 * of LinkedHashMap and is mainly used for combine.
 * */
public class RecordBlob<K, V, C> implements RecordCollection<K, V, C> {

  private final int partitionId;

  private int size = 0;
  // We can not decide the type of record value. If map combine is enabled,
  // the type of value is C, otherwise it is V.
  private LinkedHashMap<K, List<Record<K, ?>>> records = new LinkedHashMap<>();
  private List<Record<K, C>> result = new ArrayList<>();

  public RecordBlob(int partitionId) {
    this.partitionId = partitionId;
  }

  public void addRecords(RecordBuffer<K, V> recordBuffer) {
    List<Record<K,V>> recordList = recordBuffer.getRecords();
    for (Record<K, V> record : recordList) {
      K key = record.getKey();
      if (!records.containsKey(key)) {
        records.put(key, new ArrayList<>());
      }
      this.records.get(key).add(record);
      this.size++;
    }
  }

  public void addRecord(K key, V value) {
    if (!records.containsKey(key)) {
      records.put(key, new ArrayList<>());
    }
    this.records.get(key).add(Record.create(key, value));
    this.size++;
  }

  public void combine(Combiner combiner, boolean isMapCombined) {
    if (combiner == null) {
      throw new RssException("combiner is not set");
    }
    if (isMapCombined) {
      this.result = combiner.combineCombiners(records.entrySet().iterator());
    } else {
      this.result = combiner.combineValues(records.entrySet().iterator());
    }
    records.clear();
  }

  public void serialize(RecordsWriter writer) throws IOException {
    for (Record record : result) {
      writer.append(record.getKey(), record.getValue());
    }
  }

  public void clear() {
    this.size = 0;
    this.records.clear();
    this.result.clear();
  }

  public int getPartitionId() {
    return partitionId;
  }

  @Override
  public int size() {
    return this.size;
  }

  @VisibleForTesting
  public LinkedHashMap<K, List<Record<K, ?>>> getRecords() {
    return records;
  }

  public List<Record<K, C>> getResult() {
    return result;
  }
}
