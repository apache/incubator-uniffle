package org.apache.uniffle.common.serializer;

import com.google.common.base.Objects;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Comparator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.merger.AbstractSegment;
import org.apache.uniffle.common.merger.Segment;
import org.apache.uniffle.common.records.RecordsWriter;

public class SerializerUtils {

  public static class SomeClass {

    private String value;

    public SomeClass() {}

    public static SomeClass create(String value) {
      SomeClass sc = new SomeClass();
      sc.value = value;
      return sc;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SomeClass someClass = (SomeClass) o;
      return Objects.equal(value, someClass.value);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(value);
    }

    @Override
    public String toString() {
      return "SomeClass{" +
          "value='" + value + '\'' +
          '}';
    }
  }

  public static byte[] genSortedRecordBytes(RssConf rssConf, Class keyClass, Class valueClass, int start, int interval,
                                            int length, int replica) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    genSortedRecord(rssConf, keyClass, valueClass, start, interval, length, output, replica);
    return output.toByteArray();
  }

  public static AbstractSegment genMemorySegment(RssConf rssConf, Class keyClass, Class valueClass, long blockId,
                                                 int start, int interval, int length) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    genSortedRecord(rssConf, keyClass, valueClass, start, interval, length, output, 1);
    return new Segment(rssConf, output.toByteArray(), blockId, keyClass, valueClass);
  }

  public static Segment genFileSegment(RssConf rssConf, Class keyClass, Class valueClass, long blockId, int start,
                                       int interval, int length, File tmpDir) throws IOException {
    File file = new File(tmpDir, "data." + start);
    genSortedRecord(rssConf, keyClass, valueClass, start, interval, length, new FileOutputStream(file), 1);
    return new Segment(rssConf, file, 0, file.length(), blockId, keyClass, valueClass);
  }

  private static void genSortedRecord(RssConf rssConf, Class keyClass, Class valueClass, int start, int interval,
                                      int length, OutputStream output, int replica) throws IOException {
    RecordsWriter writer = new RecordsWriter(rssConf, output, keyClass, valueClass);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < replica; j ++) {
        writer.append(SerializerUtils.genData(keyClass, start + i * interval),
            SerializerUtils.genData(valueClass, start + i * interval));
      }
    }
    writer.close();
  }

  public static Object genData(Class tClass, int index) {
    if (tClass.equals(Text.class)) {
      return new Text(String.format("key%05d", index));
    } else if (tClass.equals(IntWritable.class)) {
      return new IntWritable(index);
    } else if (tClass.equals(String.class)) {
      return String.format("key%05d", index);
    } else if (tClass.equals(Integer.class)) {
      return Integer.valueOf(index);
    } else if (tClass.equals(SomeClass.class)) {
      return SomeClass.create(String.format("key%05d", index));
    } else if (tClass.equals(int.class)) {
      return index;
    }
    return null;
  }

  public static Class<?> getClassByName(String className) throws ClassNotFoundException {
    if (className.equals("int")) {
      return int.class;
    } else {
      return Class.forName(className);
    }
  }

  public static Comparator getComparator(Class tClass){
    if (tClass.equals(Text.class)) {
      return new Comparator<Text>() {
        @Override
        public int compare(Text o1, Text o2) {
          int i1 = Integer.valueOf(o1.toString().substring(3));
          int i2 = Integer.valueOf(o2.toString().substring(3));
          return i1 - i2;
        }
      };
    } else if (tClass.equals(IntWritable.class)) {
      return new Comparator<IntWritable>() {
        @Override
        public int compare(IntWritable o1, IntWritable o2) {
          return o1.get() - o2.get();
        }
      };
    } else if (tClass.equals(String.class)) {
      return new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
          int i1 = Integer.valueOf(o1.substring(3));
          int i2 = Integer.valueOf(o2.substring(3));
          return i1 - i2;
        }
      };
    } else if (tClass.equals(Integer.class)) {
      return new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
          return o1 - o2;
        }
      };
    } else if (tClass.equals(SomeClass.class)) {
      return new Comparator<SomeClass>() {
        @Override
        public int compare(SomeClass o1, SomeClass o2) {
          int i1 = Integer.valueOf(o1.value.substring(3));
          int i2 = Integer.valueOf(o2.value.substring(3));
          return i1 - i2;
        }
      };
    } else if (tClass.equals(int.class)) {
      return new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
          return o1 - o2;
        }
      };
    }
    return null;
  }
}
