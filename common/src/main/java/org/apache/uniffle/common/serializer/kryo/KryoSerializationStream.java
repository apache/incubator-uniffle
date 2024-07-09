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

package org.apache.uniffle.common.serializer.kryo;

import java.io.IOException;
import java.io.OutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeOutput;

import org.apache.uniffle.common.serializer.SerializationStream;

public class KryoSerializationStream<K, V> extends SerializationStream {

  private final KryoSerializerInstance instance;
  private Output output;
  private Kryo kryo;

  public KryoSerializationStream(KryoSerializerInstance instance, OutputStream out) {
    this.instance = instance;
    this.output = new UnsafeOutput(out);
    this.kryo = instance.borrowKryo();
  }

  @Override
  public void writeRecord(Object key, Object value) throws IOException {
    kryo.writeClassAndObject(output, key);
    kryo.writeClassAndObject(output, value);
  }

  @Override
  public void flush() throws IOException {
    if (output == null) {
      throw new IOException("Stream is closed");
    }
    output.flush();
  }

  @Override
  public void close() {
    if (output != null) {
      try {
        output.close();
      } finally {
        this.instance.releaseKryo(kryo);
        kryo = null;
        output = null;
      }
    }
  }

  @Override
  public long getTotalBytesWritten() {
    return output.total();
  }
}
