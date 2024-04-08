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

package org.apache.spark.shuffle;

import java.nio.ByteBuffer;

import scala.reflect.ClassTag$;

import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.SerializerInstance;

public class KryoSerializerWrapper {

  private volatile SerializerInstance serializerInstance;

  private static class LazyHolder {
    static final KryoSerializerWrapper INSTANCE = new KryoSerializerWrapper();
  }

  public static KryoSerializerWrapper getInstance() {
    return LazyHolder.INSTANCE;
  }

  private SerializerInstance createOrGetSerializerInstance(Class<?> tclass) {
    if (serializerInstance == null) {
      synchronized (this) {
        if (serializerInstance == null) {
          SparkConf conf = new SparkConf();
          conf.registerKryoClasses(new Class[] {tclass});
          KryoSerializer serializer = new KryoSerializer(conf);
          serializerInstance = serializer.newInstance();
        }
      }
    }
    return serializerInstance;
  }

  public ByteBuffer serialize(Object object, Class<?> tclass) {
    return createOrGetSerializerInstance(tclass).serialize(object, ClassTag$.MODULE$.apply(tclass));
  }

  public <T> T deserialize(ByteBuffer bytes, Class<T> tclass) {
    return createOrGetSerializerInstance(tclass)
        .deserialize(bytes, ClassTag$.MODULE$.apply(tclass));
  }
}
