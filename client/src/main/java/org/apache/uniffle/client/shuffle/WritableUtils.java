package org.apache.uniffle.client.shuffle;

import java.io.IOException;
import java.rmi.Remote;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.uniffle.common.exception.RssException;

public class WritableUtils {

  // Although in Remote Merge, we directly pass the Writable object itself, deep copy must be used when copying is
  // involved, because the underlying implementations of MR and TEZ copy bytes directly. Doing so allows upper-layer
  // applications to reuse Writable, avoid data errors.
  public static <T> T deepCopy(T obj) {
    try {
      if (obj instanceof NullWritable) {
        return obj;
      }
      DataOutputBuffer output = new DataOutputBuffer();
      ((Writable) obj).write(output);
      T newKey = (T) obj.getClass().newInstance();
      DataInputBuffer input = new DataInputBuffer();
      input.reset(output.getData(), 0, output.getData().length);
      ((Writable) newKey).readFields(input);
      return newKey;
    } catch (IOException | InstantiationException | IllegalAccessException e) {
      throw new RssException(e);
    }
  }
}
