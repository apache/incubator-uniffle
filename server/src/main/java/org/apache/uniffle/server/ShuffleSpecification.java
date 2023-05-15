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

package org.apache.uniffle.server;

import org.apache.uniffle.common.ShuffleDataDistributionType;

public class ShuffleSpecification {
  private int maxConcurrencyPerPartitionToWrite;
  private ShuffleDataDistributionType distributionType;

  public int getMaxConcurrencyPerPartitionToWrite() {
    return maxConcurrencyPerPartitionToWrite;
  }

  public ShuffleDataDistributionType getDistributionType() {
    return distributionType;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private ShuffleSpecification specification;

    private Builder() {
      this.specification = new ShuffleSpecification();
    }

    public Builder maxConcurrencyPerPartitionToWrite(int maxConcurrencyPerPartitionToWrite) {
      this.specification.maxConcurrencyPerPartitionToWrite = maxConcurrencyPerPartitionToWrite;
      return this;
    }

    public Builder dataDistributionType(ShuffleDataDistributionType distributionType) {
      this.specification.distributionType = distributionType;
      return this;
    }

    public ShuffleSpecification build() {
      return specification;
    }
  }
}
