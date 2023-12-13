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

package org.apache.uniffle.shuffle.utils;

import javax.annotation.Nullable;

public class CommonUtils {

  /**
   * Ensures that the target object is not null and returns it. It will throw {@link
   * NullPointerException} if the target object is null.
   */
  public static <T> T checkNotNull(T object) {
    if (object == null) {
      throw new NullPointerException("Must be not null.");
    }
    return object;
  }

  /**
   * Check the legality of method arguments. It will throw {@link IllegalArgumentException} if the
   * given condition is not true.
   */
  public static void checkArgument(boolean condition, @Nullable String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }

  public static void checkState(boolean condition, @Nullable String message) {
    if (!condition) {
      throw new IllegalStateException(message);
    }
  }
}
