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

public class RetryFactors {
  private String rpcMethod;
  private String rpcStatus;
  private int retryNumber;

  private RetryFactors() {
    // ignore
  }

  public String getRpcMethod() {
    return rpcMethod;
  }

  public String getRpcStatus() {
    return rpcStatus;
  }

  public int getRetryNumber() {
    return retryNumber;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String rpcMethod;
    private String rpcStatus;
    private int retryNumber;

    private Builder() {
      // ignore
    }

    public Builder rpcMethod(String rpcMethod) {
      this.rpcMethod = rpcMethod;
      return this;
    }

    public Builder rpcStatus(String rpcStatus) {
      this.rpcStatus = rpcStatus;
      return this;
    }

    public Builder retryNumber(int retryNumber) {
      this.retryNumber = retryNumber;
      return this;
    }

    public RetryFactors build() {
      RetryFactors retryFactors = new RetryFactors();
      retryFactors.rpcMethod = this.rpcMethod;
      retryFactors.retryNumber = this.retryNumber;
      retryFactors.rpcStatus = this.rpcStatus;
      return retryFactors;
    }
  }
}
