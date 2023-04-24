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

package org.apache.uniffle.client.impl.grpc;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.request.RssGetInMemoryShuffleDataRequest;
import org.apache.uniffle.client.request.RssGetShuffleDataRequest;
import org.apache.uniffle.client.request.RssGetShuffleIndexRequest;
import org.apache.uniffle.client.request.RssSendShuffleDataRequest;
import org.apache.uniffle.client.response.RssGetInMemoryShuffleDataResponse;
import org.apache.uniffle.client.response.RssGetShuffleDataResponse;
import org.apache.uniffle.client.response.RssGetShuffleIndexResponse;
import org.apache.uniffle.client.response.RssSendShuffleDataResponse;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.NotRetryException;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.exception.RssFetchFailedException;
import org.apache.uniffle.common.netty.client.TransportClient;
import org.apache.uniffle.common.netty.client.TransportClientFactory;
import org.apache.uniffle.common.netty.client.TransportConf;
import org.apache.uniffle.common.netty.client.TransportContext;
import org.apache.uniffle.common.netty.protocol.GetLocalShuffleDataRequest;
import org.apache.uniffle.common.netty.protocol.GetLocalShuffleDataResponse;
import org.apache.uniffle.common.netty.protocol.GetLocalShuffleIndexRequest;
import org.apache.uniffle.common.netty.protocol.GetLocalShuffleIndexResponse;
import org.apache.uniffle.common.netty.protocol.GetMemoryShuffleDataRequest;
import org.apache.uniffle.common.netty.protocol.GetMemoryShuffleDataResponse;
import org.apache.uniffle.common.netty.protocol.RpcResponse;
import org.apache.uniffle.common.netty.protocol.SendShuffleDataRequest;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.RetryUtils;

public class ShuffleServerGrpcNettyClient extends ShuffleServerGrpcClient {
  private static final Logger LOG = LoggerFactory.getLogger(ShuffleServerGrpcNettyClient.class);
  private int nettyPort;
  private TransportClientFactory clientFactory;

  public ShuffleServerGrpcNettyClient(RssConf rssConf, String host, int grpcPort, int nettyPort) {
    super(host, grpcPort);
    this.nettyPort = nettyPort;
    TransportContext transportContext = new TransportContext(new TransportConf(rssConf));
    this.clientFactory = new TransportClientFactory(transportContext);
  }

  @Override
  public RssSendShuffleDataResponse sendShuffleData(RssSendShuffleDataRequest request) {
    TransportClient transportClient = getTransportClient();
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleIdToBlocks = request.getShuffleIdToBlocks();
    boolean isSuccessful = true;

    for (Map.Entry<Integer, Map<Integer, List<ShuffleBlockInfo>>> stb : shuffleIdToBlocks.entrySet()) {
      int shuffleId = stb.getKey();
      int size = 0;
      int blockNum = 0;
      for (Map.Entry<Integer, List<ShuffleBlockInfo>> ptb : stb.getValue().entrySet()) {
        for (ShuffleBlockInfo sbi : ptb.getValue()) {
          size += sbi.getSize();
          blockNum++;
        }
      }

      int allocateSize = size;
      int finalBlockNum = blockNum;
      try {
        RetryUtils.retry(() -> {
          long requireId = requirePreAllocation(allocateSize, request.getRetryMax(), request.getRetryIntervalMax());
          if (requireId == FAILED_REQUIRE_ID) {
            throw new RssException(String.format(
                "requirePreAllocation failed! size[%s], host[%s], port[%s]", allocateSize, host, port));
          }

          SendShuffleDataRequest sendShuffleDataRequest = new SendShuffleDataRequest(
              requestId(),
              request.getAppId(),
              shuffleId,
              requireId,
              stb.getValue(),
              System.currentTimeMillis());
          long start = System.currentTimeMillis();
          RpcResponse rpcResponse = transportClient.sendRpcSync(sendShuffleDataRequest, RPC_TIMEOUT_DEFAULT_MS);
          LOG.debug("Do sendShuffleData to {}:{} rpc cost:" + (System.currentTimeMillis() - start)
                        + " ms for " + allocateSize + " bytes with " + finalBlockNum + " blocks", host, port);
          if (rpcResponse.getStatusCode() != StatusCode.SUCCESS) {
            String msg = "Can't send shuffle data with " + finalBlockNum
                             + " blocks to " + host + ":" + port
                             + ", statusCode=" + rpcResponse.getStatusCode()
                             + ", errorMsg:" + rpcResponse.getRetMessage();
            if (rpcResponse.getStatusCode() == StatusCode.NO_REGISTER) {
              throw new NotRetryException(msg);
            } else {
              throw new RssException(msg);
            }
          }
          return rpcResponse;
        }, request.getRetryIntervalMax(), maxRetryAttempts);
      } catch (Throwable throwable) {
        LOG.warn(throwable.getMessage());
        isSuccessful = false;
        break;
      }
    }

    RssSendShuffleDataResponse response;
    if (isSuccessful) {
      response = new RssSendShuffleDataResponse(StatusCode.SUCCESS);
    } else {
      response = new RssSendShuffleDataResponse(StatusCode.INTERNAL_ERROR);
    }
    return response;
  }

  @Override
  public RssGetInMemoryShuffleDataResponse getInMemoryShuffleData(
      RssGetInMemoryShuffleDataRequest request) {
    TransportClient transportClient = getTransportClient();
    GetMemoryShuffleDataRequest getMemoryShuffleDataRequest = new GetMemoryShuffleDataRequest(
        requestId(),
        request.getAppId(),
        request.getShuffleId(),
        request.getPartitionId(),
        request.getLastBlockId(),
        request.getReadBufferSize(),
        System.currentTimeMillis(),
        request.getExpectedTaskIds()
    );
    String requestInfo = "appId[" + request.getAppId()
                             + "], shuffleId[" + request.getShuffleId()
                             + "], partitionId[" + request.getPartitionId()
                             + "], lastBlockId[" + request.getLastBlockId() + "]";
    RpcResponse rpcResponse = transportClient.sendRpcSync(getMemoryShuffleDataRequest, RPC_TIMEOUT_DEFAULT_MS);
    GetMemoryShuffleDataResponse getMemoryShuffleDataResponse = (GetMemoryShuffleDataResponse) rpcResponse;
    StatusCode statusCode = rpcResponse.getStatusCode();
    switch (statusCode) {
      case SUCCESS:
        return new RssGetInMemoryShuffleDataResponse(
            StatusCode.SUCCESS, getMemoryShuffleDataResponse.getData().nioBuffer(),
            getMemoryShuffleDataResponse.getBufferSegments());
      default:
        String msg = "Can't get shuffle in memory data from " + host + ":" + port
                         + " for " + requestInfo + ", errorMsg:" + getMemoryShuffleDataResponse.getRetMessage();
        LOG.error(msg);
        throw new RssFetchFailedException(msg);
    }
  }

  @Override
  public RssGetShuffleIndexResponse getShuffleIndex(RssGetShuffleIndexRequest request) {
    TransportClient transportClient = getTransportClient();
    GetLocalShuffleIndexRequest getLocalShuffleIndexRequest = new GetLocalShuffleIndexRequest(
        requestId(),
        request.getAppId(),
        request.getShuffleId(),
        request.getPartitionId(),
        request.getPartitionNumPerRange(),
        request.getPartitionNum()
    );
    long start = System.currentTimeMillis();
    RpcResponse rpcResponse = transportClient.sendRpcSync(getLocalShuffleIndexRequest, RPC_TIMEOUT_DEFAULT_MS);
    String requestInfo = "appId[" + request.getAppId()
                             + "], shuffleId[" + request.getShuffleId()
                             + "], partitionId[" + request.getPartitionId();
    LOG.info("GetShuffleIndex from {}:{} for {} cost {} ms", host, port,
        requestInfo, System.currentTimeMillis() - start);
    GetLocalShuffleIndexResponse getLocalShuffleIndexResponse = (GetLocalShuffleIndexResponse) rpcResponse;
    StatusCode statusCode = rpcResponse.getStatusCode();
    switch (statusCode) {
      case SUCCESS:
        return new RssGetShuffleIndexResponse(
            StatusCode.SUCCESS,
            getLocalShuffleIndexResponse.getIndexData().nioBuffer(),
            getLocalShuffleIndexResponse.getFileLength());
      default:
        String msg = "Can't get shuffle index from " + host + ":" + port
                         + " for " + requestInfo + ", errorMsg:" + getLocalShuffleIndexResponse.getRetMessage();
        LOG.error(msg);
        throw new RssFetchFailedException(msg);
    }
  }

  @Override
  public RssGetShuffleDataResponse getShuffleData(RssGetShuffleDataRequest request) {
    TransportClient transportClient = getTransportClient();
    GetLocalShuffleDataRequest getLocalShuffleIndexRequest = new GetLocalShuffleDataRequest(
        requestId(),
        request.getAppId(),
        request.getShuffleId(),
        request.getPartitionId(),
        request.getPartitionNumPerRange(),
        request.getPartitionNum(),
        request.getOffset(),
        request.getLength(),
        System.currentTimeMillis()
    );
    long start = System.currentTimeMillis();
    RpcResponse rpcResponse = transportClient.sendRpcSync(getLocalShuffleIndexRequest, RPC_TIMEOUT_DEFAULT_MS);
    String requestInfo = "appId[" + request.getAppId() + "], shuffleId["
                             + request.getShuffleId() + "], partitionId[" + request.getPartitionId() + "]";
    LOG.info("GetShuffleData from {}:{} for {} cost {} ms", host, port, requestInfo,
        System.currentTimeMillis() - start);
    GetLocalShuffleDataResponse getLocalShuffleDataResponse  = (GetLocalShuffleDataResponse) rpcResponse;
    StatusCode statusCode = rpcResponse.getStatusCode();
    switch (statusCode) {
      case SUCCESS:
        return new RssGetShuffleDataResponse(
            StatusCode.SUCCESS,
            getLocalShuffleDataResponse.getData().nioBuffer());
      default:
        String msg = "Can't get shuffle data from " + host + ":" + port
                         + " for " + requestInfo + ", errorMsg:" + getLocalShuffleDataResponse.getRetMessage();
        LOG.error(msg);
        throw new RssFetchFailedException(msg);
    }
  }

  private static final AtomicLong counter = new AtomicLong();

  public static long requestId() {
    return counter.getAndIncrement();
  }

  private TransportClient getTransportClient() {
    TransportClient transportClient;
    try {
      transportClient = clientFactory.createClient(host, nettyPort);
    } catch (Exception e) {
      throw new RssException("create transport client failed", e);
    }
    return transportClient;
  }

}
