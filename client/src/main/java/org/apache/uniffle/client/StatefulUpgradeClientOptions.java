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

package org.apache.uniffle.client;

public class StatefulUpgradeClientOptions {
  private boolean statefulUpgradeEnable;
  private int retryMaxNumber = 150;
  private int retryIntervalMax = 2000;
  private int backoffBase = 2000;

  private StatefulUpgradeClientOptions() {
    // ignore
  }

  public boolean isStatefulUpgradeEnable() {
    return statefulUpgradeEnable;
  }

  public int getRetryMaxNumber() {
    return retryMaxNumber;
  }

  public int getRetryIntervalMax() {
    return retryIntervalMax;
  }

  public int getBackoffBase() {
    return backoffBase;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private StatefulUpgradeClientOptions options = new StatefulUpgradeClientOptions();

    private Builder() {
      // ignore
    }

    public Builder statefulUpgradeEnable(boolean statefulUpgradeEnable) {
      options.statefulUpgradeEnable = statefulUpgradeEnable;
      return this;
    }

    public Builder retryMaxNumber(int retryMaxNumber) {
      options.retryMaxNumber = retryMaxNumber;
      return this;
    }

    public Builder retryIntervalMax(int retryIntervalMax) {
      options.retryIntervalMax = retryIntervalMax;
      return this;
    }

    public Builder backoffBase(int backoffBase) {
      options.backoffBase = backoffBase;
      return this;
    }

    public StatefulUpgradeClientOptions build() {
      return options;
    }
  }
}
