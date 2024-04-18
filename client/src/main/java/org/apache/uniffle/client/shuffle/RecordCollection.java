package org.apache.uniffle.client.shuffle;

import java.io.IOException;
import org.apache.uniffle.common.records.RecordsWriter;

public interface RecordCollection<K, V, C> {

  void addRecord(K key, V value);

  void serialize(RecordsWriter<K, C> writer) throws IOException;

  int size();
}
