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

package org.apache.uniffle.common.util.http;

import org.apache.http.impl.client.CloseableHttpClient;

public class UniffleRestClient implements AutoCloseable {
  private RestClient restClient;
  private RestClientConf conf;
  // This is the base host URL for the coordinator server.
  // It should be in the format of "https://server:port" or "http://server:port".
  private String hostUrl;

  @Override
  public void close() throws Exception {
    if (restClient != null) {
      restClient.close();
    }
  }

  private UniffleRestClient(Builder builder) {
    this.hostUrl = builder.hostUrl;

    RestClientConf conf = new RestClientConf();
    conf.setConnectTimeout(builder.connectTimeout);
    conf.setSocketTimeout(builder.socketTimeout);
    conf.setMaxAttempts(builder.maxAttempts);
    conf.setAttemptWaitTime(builder.attemptWaitTime);
    this.conf = conf;
    CloseableHttpClient httpclient = HttpClientFactory.createHttpClient(conf);
    this.restClient = new RestClientImpl(hostUrl, httpclient);
  }

  public RestClient getHttpClient() {
    return restClient;
  }

  public RestClientConf getConf() {
    return conf;
  }

  public static Builder builder(String hostUrl) {
    return new Builder(hostUrl);
  }

  public static class Builder {

    private String hostUrl;

    // 2 minutes
    private int socketTimeout = 2 * 60 * 1000;

    // 30s
    private int connectTimeout = 30 * 1000;

    private int maxAttempts = 3;

    // 3s
    private int attemptWaitTime = 3 * 1000;

    public Builder(String hostUrl) {
      this.hostUrl = hostUrl;
    }

    public Builder socketTimeout(int socketTimeout) {
      this.socketTimeout = socketTimeout;
      return this;
    }

    public Builder connectionTimeout(int connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    public Builder maxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    public Builder attemptWaitTime(int attemptWaitTime) {
      this.attemptWaitTime = attemptWaitTime;
      return this;
    }

    public UniffleRestClient build() {
      return new UniffleRestClient(this);
    }
  }
}
