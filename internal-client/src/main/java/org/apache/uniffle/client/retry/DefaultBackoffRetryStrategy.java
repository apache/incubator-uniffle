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

package org.apache.uniffle.client.retry;

import java.time.Duration;
import java.util.Random;
import java.util.function.Function;

public class DefaultBackoffRetryStrategy implements RetryStrategy {
  private Function<RetryFactors, Boolean> allowRetryFunction;
  private int retryMaxNumber;
  private int retryIntervalMax;
  private int backoffBase;

  private Random random;

  public DefaultBackoffRetryStrategy(Function<RetryFactors, Boolean> allowRetryFunction,
      int retryMaxNumber, int retryIntervalMax, int backOffBase) {
    this.allowRetryFunction = allowRetryFunction;
    this.retryMaxNumber = retryMaxNumber;
    this.retryIntervalMax = retryIntervalMax;
    this.backoffBase = backOffBase;
    this.random = new Random();
  }

  @Override
  public Duration getRetryDelay(RetryFactors retryFactors) {
    if (!allowRetryFunction.apply(retryFactors)) {
      return Duration.ZERO;
    }
    int retryNumber = retryFactors.getRetryNumber();
    if (retryNumber >= retryMaxNumber) {
      return Duration.ZERO;
    }
    long backoffTime = Math.min(retryIntervalMax,
        backoffBase * (1L << Math.min(retryNumber, 16)) + random.nextInt(backoffBase));
    return Duration.ofMillis(backoffTime);
  }
}
