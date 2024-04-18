package org.apache.spark.shuffle.reader;

import java.io.IOException;
import org.apache.uniffle.client.shuffle.Record;
import org.apache.uniffle.client.shuffle.reader.KeyValueReader;
import org.apache.uniffle.client.shuffle.reader.RMRecordsReader;
import org.apache.uniffle.common.exception.RssException;
import scala.collection.AbstractIterator;
import scala.runtime.BoxedUnit;

public class RMRssShuffleDataIterator <K, C> extends AbstractIterator<Record<K, C>> {

  private RMRecordsReader<K, ?, C> reader;
  private KeyValueReader<K, C> keyValueReader;

  public RMRssShuffleDataIterator(RMRecordsReader<K, ?, C> reader) {
    this.reader = reader;
    this.keyValueReader = reader.keyValueReader();
  }

  @Override
  public boolean hasNext() {
    try {
      return this.keyValueReader.next();
    } catch (IOException e) {
      throw new RssException(e);
    }
  }

  @Override
  public Record<K, C> next() {
    try {
      return Record.create(this.keyValueReader.getCurrentKey(), this.keyValueReader.getCurrentValue());
    } catch (IOException e) {
      throw new RssException(e);
    }
  }

  public BoxedUnit cleanup() {
    reader.close();
    return BoxedUnit.UNIT;
  }
}
