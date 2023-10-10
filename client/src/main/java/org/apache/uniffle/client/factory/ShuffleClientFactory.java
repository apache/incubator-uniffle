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

package org.apache.uniffle.client.factory;

import org.apache.uniffle.client.api.ShuffleReadClient;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.impl.ShuffleReadClientImpl;
import org.apache.uniffle.client.impl.ShuffleWriteClientImpl;
import org.apache.uniffle.client.request.CreateShuffleReadClientRequest;
import org.apache.uniffle.common.config.RssConf;

public class ShuffleClientFactory {

  private static final ShuffleClientFactory INSTANCE = new ShuffleClientFactory();

  private ShuffleClientFactory() {}

  public static ShuffleClientFactory getInstance() {
    return INSTANCE;
  }

  public ShuffleWriteClient createShuffleWriteClient(WriteClientBuilder builder) {
    if (builder.isReplicaSkipEnabled() && builder.getReplica() > builder.getReplicaWrite()) {
      builder.retryMax(builder.getRetryMax() / 2);
    }
    return builder.build();
  }

  public ShuffleReadClient createShuffleReadClient(CreateShuffleReadClientRequest request) {
    return new ShuffleReadClientImpl(
        request.getAppId(),
        request.getShuffleId(),
        request.getPartitionId(),
        request.getPartitionNumPerRange(),
        request.getPartitionNum(),
        request.getBasePath(),
        request.getBlockIdBitmap(),
        request.getTaskIdBitmap(),
        request.getShuffleServerInfoList(),
        request.getHadoopConf(),
        request.getIdHelper(),
        request.getShuffleDataDistributionType(),
        request.isExpectedTaskIdsBitmapFilterEnable(),
        request.getRssConf());
  }

  public static class WriteClientBuilder {
    private WriteClientBuilder() {}

    private String clientType;
    private int retryMax;
    private long retryIntervalMax;
    private int heartBeatThreadNum;
    private int replica;
    private int replicaWrite;
    private int replicaRead;
    private boolean replicaSkipEnabled;
    private int dataTransferPoolSize;
    private int dataCommitPoolSize;
    private int unregisterThreadPoolSize;
    private int unregisterRequestTimeSec;
    private RssConf rssConf;

    public String getClientType() {
      return clientType;
    }

    public int getRetryMax() {
      return retryMax;
    }

    public long getRetryIntervalMax() {
      return retryIntervalMax;
    }

    public int getHeartBeatThreadNum() {
      return heartBeatThreadNum;
    }

    public int getReplica() {
      return replica;
    }

    public int getReplicaWrite() {
      return replicaWrite;
    }

    public int getReplicaRead() {
      return replicaRead;
    }

    public boolean isReplicaSkipEnabled() {
      return replicaSkipEnabled;
    }

    public int getDataTransferPoolSize() {
      return dataTransferPoolSize;
    }

    public int getDataCommitPoolSize() {
      return dataCommitPoolSize;
    }

    public int getUnregisterThreadPoolSize() {
      return unregisterThreadPoolSize;
    }

    public int getUnregisterRequestTimeSec() {
      return unregisterRequestTimeSec;
    }

    public RssConf getRssConf() {
      return rssConf;
    }

    public WriteClientBuilder clientType(String clientType) {
      this.clientType = clientType;
      return this;
    }

    public WriteClientBuilder retryMax(int retryMax) {
      this.retryMax = retryMax;
      return this;
    }

    public WriteClientBuilder retryIntervalMax(long retryIntervalMax) {
      this.retryIntervalMax = retryIntervalMax;
      return this;
    }

    public WriteClientBuilder heartBeatThreadNum(int heartBeatThreadNum) {
      this.heartBeatThreadNum = heartBeatThreadNum;
      return this;
    }

    public WriteClientBuilder replica(int replica) {
      this.replica = replica;
      return this;
    }

    public WriteClientBuilder replicaWrite(int replicaWrite) {
      this.replicaWrite = replicaWrite;
      return this;
    }

    public WriteClientBuilder replicaRead(int replicaRead) {
      this.replicaRead = replicaRead;
      return this;
    }

    public WriteClientBuilder replicaSkipEnabled(boolean replicaSkipEnabled) {
      this.replicaSkipEnabled = replicaSkipEnabled;
      return this;
    }

    public WriteClientBuilder dataTransferPoolSize(int dataTransferPoolSize) {
      this.dataTransferPoolSize = dataTransferPoolSize;
      return this;
    }

    public WriteClientBuilder dataCommitPoolSize(int dataCommitPoolSize) {
      this.dataCommitPoolSize = dataCommitPoolSize;
      return this;
    }

    public WriteClientBuilder unregisterThreadPoolSize(int unregisterThreadPoolSize) {
      this.unregisterThreadPoolSize = unregisterThreadPoolSize;
      return this;
    }

    public WriteClientBuilder unregisterRequestTimeSec(int unregisterRequestTimeSec) {
      this.unregisterRequestTimeSec = unregisterRequestTimeSec;
      return this;
    }

    public WriteClientBuilder rssConf(RssConf rssConf) {
      this.rssConf = rssConf;
      return this;
    }

    public ShuffleWriteClientImpl build() {
      return new ShuffleWriteClientImpl(this);
    }
  }

  public static WriteClientBuilder newWriteBuilder() {
    return new WriteClientBuilder();
  }
}
