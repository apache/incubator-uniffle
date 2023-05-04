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

package org.apache.uniffle.common.util;

import java.lang.reflect.Constructor;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.uniffle.common.exception.RssException;

public class ClassUtils {

  @SuppressWarnings("unchecked")
  public static <T> T instantiate(Class<T> clazz, Pair<Class<T>, Object>... typeAndVals)
      throws RssException {
    try {
      if (typeAndVals == null || typeAndVals.length == 0) {
        return clazz.getConstructor().newInstance();
      }
      Class<T>[] types = Stream.of(typeAndVals).map(x -> x.getLeft()).toArray(Class[]::new);
      Constructor<T> constructor = clazz.getConstructor(types);
      return constructor.newInstance(
          Stream.of(typeAndVals).map(x -> x.getRight()).toArray(Object[]::new)
      );
    } catch (Exception e) {
      throw new RssException(e);
    }
  }
}
