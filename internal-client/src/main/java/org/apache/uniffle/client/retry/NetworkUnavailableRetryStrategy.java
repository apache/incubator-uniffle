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

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkUnavailableRetryStrategy implements RetryStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(NetworkUnavailableRetryStrategy.class);
  private static final String STATUS_CODE = "UNAVAILABLE";

  private int retryMaxNumber;
  private int retryIntervalMax;
  private int backOffBase;

  private Random random;

  public NetworkUnavailableRetryStrategy(int retryMaxNumber, int retryIntervalMax, int backOffBase) {
    this.retryMaxNumber = retryMaxNumber;
    this.retryIntervalMax = retryIntervalMax;
    this.backOffBase = backOffBase;
    this.random = new Random();
  }

  @Override
  public boolean needToRetry(String status, int retryNumber) {
    try {
      if (STATUS_CODE.equalsIgnoreCase(status) && retryNumber < retryMaxNumber) {
        long backoffTime =
            Math.min(retryIntervalMax, backOffBase * (1L << Math.min(retryNumber, 16)) + random.nextInt(backOffBase));
        LOGGER.info("Sleep: {}", backoffTime);
        Thread.sleep(backoffTime);
        return true;
      }
    } catch (Exception e) {
      LOGGER.error("Failed to apply {}. Status: {}, retryNumber: {}",
          this.getClass().getSimpleName(), status, retryNumber, e);
    }
    return false;
  }
}
