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

package org.apache.uniffle.coordinator;

import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.Time;

public class GenericTestUtils {

  public static final String ERROR_MISSING_ARGUMENT =
      "Input supplier interface should be initailized";
  public static final String ERROR_INVALID_ARGUMENT =
      "Total wait time should be greater than check interval time";

  public static void waitFor(
      final Supplier<Boolean> check, final long checkEveryMillis, final long waitForMillis)
      throws TimeoutException, InterruptedException {
    waitFor(check, checkEveryMillis, waitForMillis, null);
  }

  public static void waitFor(
      final Supplier<Boolean> check,
      final long checkEveryMillis,
      final long waitForMillis,
      final String errorMsg)
      throws TimeoutException, InterruptedException {
    Objects.requireNonNull(check, ERROR_MISSING_ARGUMENT);
    if (waitForMillis < checkEveryMillis) {
      throw new IllegalArgumentException(ERROR_INVALID_ARGUMENT);
    }

    long st = Time.monotonicNow();
    boolean result = check.get();

    while (!result && (Time.monotonicNow() - st < waitForMillis)) {
      Thread.sleep(checkEveryMillis);
      result = check.get();
    }

    if (!result) {
      final String exceptionErrorMsg =
          "Timed out waiting for condition. "
              + (StringUtils.isNotEmpty(errorMsg) ? "Error Message: " + errorMsg : "");
      throw new TimeoutException(exceptionErrorMsg);
    }
  }
}
