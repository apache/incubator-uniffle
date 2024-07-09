/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.uniffle.common.serializer;

import java.util.Comparator;

import com.google.common.base.Objects;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

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
      return "SomeClass{" + "value='" + value + '\'' + '}';
    }
  }

  public static Object genData(Class tClass, int index) {
    if (tClass.equals(Text.class)) {
      return new Text(String.format("key%08d", index));
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

  public static Comparator getComparator(Class tClass) {
    if (tClass.equals(Text.class)) {
      return new Text.Comparator();
    } else if (tClass.equals(IntWritable.class)) {
      return new IntWritable.Comparator();
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
