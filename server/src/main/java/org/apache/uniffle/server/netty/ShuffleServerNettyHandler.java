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

package org.apache.uniffle.server.netty;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.ShuffleIndexResult;
import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.ShufflePartitionedData;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.exception.FileNotFoundException;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.netty.buffer.ManagedBuffer;
import org.apache.uniffle.common.netty.buffer.NettyManagedBuffer;
import org.apache.uniffle.common.netty.client.TransportClient;
import org.apache.uniffle.common.netty.handle.BaseMessageHandler;
import org.apache.uniffle.common.netty.protocol.GetLocalShuffleDataRequest;
import org.apache.uniffle.common.netty.protocol.GetLocalShuffleDataResponse;
import org.apache.uniffle.common.netty.protocol.GetLocalShuffleIndexRequest;
import org.apache.uniffle.common.netty.protocol.GetLocalShuffleIndexResponse;
import org.apache.uniffle.common.netty.protocol.GetMemoryShuffleDataRequest;
import org.apache.uniffle.common.netty.protocol.GetMemoryShuffleDataResponse;
import org.apache.uniffle.common.netty.protocol.RequestMessage;
import org.apache.uniffle.common.netty.protocol.RpcResponse;
import org.apache.uniffle.common.netty.protocol.SendShuffleDataRequest;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.server.ShuffleDataReadEvent;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.ShuffleServerMetrics;
import org.apache.uniffle.server.ShuffleTaskManager;
import org.apache.uniffle.server.buffer.PreAllocatedBufferInfo;
import org.apache.uniffle.server.buffer.ShuffleBufferManager;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.common.StorageReadMetrics;
import org.apache.uniffle.storage.util.ShuffleStorageUtils;

public class ShuffleServerNettyHandler implements BaseMessageHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleServerNettyHandler.class);
  private static final int RPC_TIMEOUT = 60000;
  private final ShuffleServer shuffleServer;

  public ShuffleServerNettyHandler(ShuffleServer shuffleServer) {
    this.shuffleServer = shuffleServer;
  }

  @Override
  public void receive(TransportClient client, RequestMessage msg) {
    shuffleServer.getNettyMetrics().incCounter(msg.getClass().getName());
    if (msg instanceof SendShuffleDataRequest) {
      handleSendShuffleDataRequest(client, (SendShuffleDataRequest) msg);
    } else if (msg instanceof GetLocalShuffleDataRequest) {
      handleGetLocalShuffleData(client, (GetLocalShuffleDataRequest) msg);
    } else if (msg instanceof GetLocalShuffleIndexRequest) {
      handleGetLocalShuffleIndexRequest(client, (GetLocalShuffleIndexRequest) msg);
    } else if (msg instanceof GetMemoryShuffleDataRequest) {
      handleGetMemoryShuffleDataRequest(client, (GetMemoryShuffleDataRequest) msg);
    } else {
      throw new RssException("Can not handle message " + msg.type());
    }
    shuffleServer.getNettyMetrics().decCounter(msg.getClass().getName());
  }

  @Override
  public void exceptionCaught(Throwable cause, TransportClient client) {
    LOG.error("exception caught {}", client.getSocketAddress(), cause);
  }

  public void handleSendShuffleDataRequest(TransportClient client, SendShuffleDataRequest req) {
    RpcResponse rpcResponse;
    String appId = req.getAppId();
    int shuffleId = req.getShuffleId();
    long requireBufferId = req.getRequireId();
    long timestamp = req.getTimestamp();
    if (timestamp > 0) {
      /*
       * Here we record the transport time, but we don't consider the impact of data size on transport time.
       * The amount of data will not cause great fluctuations in latency. For example, 100K costs 1ms,
       * and 1M costs 10ms. This seems like a normal fluctuation, but it may rise to 10s when the server load is high.
       * In addition, we need to pay attention to that the time of the client machine and the machine
       * time of the Shuffle Server should be kept in sync. TransportTime is accurate only if this condition is met.
       * */
      long transportTime = System.currentTimeMillis() - timestamp;
      if (transportTime > 0) {
        shuffleServer
            .getNettyMetrics()
            .recordTransportTime(SendShuffleDataRequest.class.getName(), transportTime);
      }
    }
    int requireSize = shuffleServer.getShuffleTaskManager().getRequireBufferSize(requireBufferId);
    int requireBlocksSize =
        requireSize - req.encodedLength() < 0 ? 0 : requireSize - req.encodedLength();

    StatusCode ret = StatusCode.SUCCESS;
    String responseMessage = "OK";
    if (req.getPartitionToBlocks().size() > 0) {
      ShuffleServerMetrics.counterTotalReceivedDataSize.inc(requireBlocksSize);
      ShuffleTaskManager manager = shuffleServer.getShuffleTaskManager();
      PreAllocatedBufferInfo info = manager.getAndRemovePreAllocatedBuffer(requireBufferId);
      boolean isPreAllocated = info != null;
      if (!isPreAllocated) {
        req.getPartitionToBlocks().values().stream()
            .flatMap(Collection::stream)
            .forEach(block -> block.getData().release());

        String errorMsg =
            "Can't find requireBufferId["
                + requireBufferId
                + "] for appId["
                + appId
                + "], shuffleId["
                + shuffleId
                + "], probably because the pre-allocated buffer has expired. "
                + "Please increase the expiration time using "
                + ShuffleServerConf.SERVER_PRE_ALLOCATION_EXPIRED.key()
                + " in ShuffleServer's configuration";
        LOG.warn(errorMsg);
        rpcResponse = new RpcResponse(req.getRequestId(), StatusCode.INTERNAL_ERROR, errorMsg);
        client.getChannel().writeAndFlush(rpcResponse);
        return;
      }
      final long start = System.currentTimeMillis();
      ShuffleBufferManager shuffleBufferManager = shuffleServer.getShuffleBufferManager();
      shuffleBufferManager.releaseMemory(req.encodedLength(), false, true);
      List<ShufflePartitionedData> shufflePartitionedData = toPartitionedData(req);
      long alreadyReleasedSize = 0;
      boolean hasFailureOccurred = false;
      for (ShufflePartitionedData spd : shufflePartitionedData) {
        String shuffleDataInfo =
            "appId["
                + appId
                + "], shuffleId["
                + shuffleId
                + "], partitionId["
                + spd.getPartitionId()
                + "]";
        try {
          if (hasFailureOccurred) {
            continue;
          }
          ret = manager.cacheShuffleData(appId, shuffleId, isPreAllocated, spd);
          if (ret != StatusCode.SUCCESS) {
            String errorMsg =
                "Error happened when shuffleEngine.write for "
                    + shuffleDataInfo
                    + ", statusCode="
                    + ret;
            LOG.error(errorMsg);
            responseMessage = errorMsg;
            hasFailureOccurred = true;
          } else {
            long toReleasedSize = spd.getTotalBlockSize();
            // after each cacheShuffleData call, the `preAllocatedSize` is updated timely.
            manager.releasePreAllocatedSize(toReleasedSize);
            alreadyReleasedSize += toReleasedSize;
            manager.updateCachedBlockIds(
                appId, shuffleId, spd.getPartitionId(), spd.getBlockList());
          }
        } catch (Exception e) {
          String errorMsg =
              "Error happened when shuffleEngine.write for "
                  + shuffleDataInfo
                  + ": "
                  + e.getMessage();
          ret = StatusCode.INTERNAL_ERROR;
          responseMessage = errorMsg;
          LOG.error(errorMsg);
          hasFailureOccurred = true;
        } finally {
          // Once the cache failure occurs, we should explicitly release data held by byteBuf
          if (hasFailureOccurred) {
            Arrays.stream(spd.getBlockList()).forEach(block -> block.getData().release());
            shuffleBufferManager.releaseMemory(spd.getTotalBlockSize(), false, false);
          }
        }
      }
      // since the required buffer id is only used once, the shuffle client would try to require
      // another buffer whether
      // current connection succeeded or not. Therefore, the preAllocatedBuffer is first get and
      // removed, then after
      // cacheShuffleData finishes, the preAllocatedSize should be updated accordingly.
      if (requireBlocksSize > alreadyReleasedSize) {
        manager.releasePreAllocatedSize(requireBlocksSize - alreadyReleasedSize);
      }
      rpcResponse = new RpcResponse(req.getRequestId(), ret, responseMessage);
      long costTime = System.currentTimeMillis() - start;
      shuffleServer
          .getNettyMetrics()
          .recordProcessTime(SendShuffleDataRequest.class.getName(), costTime);
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Cache Shuffle Data for appId["
                + appId
                + "], shuffleId["
                + shuffleId
                + "], cost "
                + costTime
                + " ms with "
                + shufflePartitionedData.size()
                + " blocks and "
                + requireBlocksSize
                + " bytes");
      }
    } else {
      rpcResponse =
          new RpcResponse(req.getRequestId(), StatusCode.INTERNAL_ERROR, "No data in request");
    }

    client.getChannel().writeAndFlush(rpcResponse);
  }

  public void handleGetMemoryShuffleDataRequest(
      TransportClient client, GetMemoryShuffleDataRequest req) {
    String appId = req.getAppId();
    int shuffleId = req.getShuffleId();
    int partitionId = req.getPartitionId();
    long blockId = req.getLastBlockId();
    int readBufferSize = req.getReadBufferSize();
    long timestamp = req.getTimestamp();

    if (timestamp > 0) {
      long transportTime = System.currentTimeMillis() - timestamp;
      if (transportTime > 0) {
        shuffleServer
            .getNettyMetrics()
            .recordTransportTime(GetMemoryShuffleDataRequest.class.getName(), transportTime);
      }
    }
    final long start = System.currentTimeMillis();
    StatusCode status = StatusCode.SUCCESS;
    String msg = "OK";
    GetMemoryShuffleDataResponse response;
    String requestInfo =
        "appId[" + appId + "], shuffleId[" + shuffleId + "], partitionId[" + partitionId + "]";

    // todo: if can get the exact memory size?
    if (shuffleServer.getShuffleBufferManager().requireReadMemory(readBufferSize)) {
      ShuffleDataResult shuffleDataResult = null;
      try {
        shuffleDataResult =
            shuffleServer
                .getShuffleTaskManager()
                .getInMemoryShuffleData(
                    appId,
                    shuffleId,
                    partitionId,
                    blockId,
                    readBufferSize,
                    req.getExpectedTaskIdsBitmap());
        ManagedBuffer data = NettyManagedBuffer.EMPTY_BUFFER;
        List<BufferSegment> bufferSegments = Lists.newArrayList();
        if (shuffleDataResult != null) {
          data = shuffleDataResult.getManagedBuffer();
          bufferSegments = shuffleDataResult.getBufferSegments();
          ShuffleServerMetrics.counterTotalReadDataSize.inc(data.size());
          ShuffleServerMetrics.counterTotalReadMemoryDataSize.inc(data.size());
        }
        response =
            new GetMemoryShuffleDataResponse(req.getRequestId(), status, msg, bufferSegments, data);
        ReleaseMemoryAndRecordReadTimeListener listener =
            new ReleaseMemoryAndRecordReadTimeListener(
                start, readBufferSize, data.size(), requestInfo, req, client);
        client.getChannel().writeAndFlush(response).addListener(listener);
        return;
      } catch (Exception e) {
        shuffleServer.getShuffleBufferManager().releaseReadMemory(readBufferSize);
        if (shuffleDataResult != null) {
          shuffleDataResult.release();
        }
        status = StatusCode.INTERNAL_ERROR;
        msg =
            "Error happened when get in memory shuffle data for "
                + requestInfo
                + ", "
                + e.getMessage();
        LOG.error(msg, e);
        response =
            new GetMemoryShuffleDataResponse(
                req.getRequestId(), status, msg, Lists.newArrayList(), Unpooled.EMPTY_BUFFER);
      }
    } else {
      status = StatusCode.NO_BUFFER;
      msg = "Can't require memory to get in memory shuffle data";
      LOG.error(msg + " for " + requestInfo);
      response =
          new GetMemoryShuffleDataResponse(
              req.getRequestId(), status, msg, Lists.newArrayList(), Unpooled.EMPTY_BUFFER);
    }
    client.getChannel().writeAndFlush(response);
  }

  public void handleGetLocalShuffleIndexRequest(
      TransportClient client, GetLocalShuffleIndexRequest req) {
    String appId = req.getAppId();
    int shuffleId = req.getShuffleId();
    int partitionId = req.getPartitionId();
    int partitionNumPerRange = req.getPartitionNumPerRange();
    int partitionNum = req.getPartitionNum();
    StatusCode status = StatusCode.SUCCESS;
    String msg = "OK";
    GetLocalShuffleIndexResponse response;
    String requestInfo =
        "appId[" + appId + "], shuffleId[" + shuffleId + "], partitionId[" + partitionId + "]";

    int[] range =
        ShuffleStorageUtils.getPartitionRange(partitionId, partitionNumPerRange, partitionNum);
    Storage storage =
        shuffleServer
            .getStorageManager()
            .selectStorage(new ShuffleDataReadEvent(appId, shuffleId, partitionId, range[0]));
    if (storage != null) {
      storage.updateReadMetrics(new StorageReadMetrics(appId, shuffleId));
    }
    // Index file is expected small size and won't cause oom problem with the assumed size. An index
    // segment is 40B,
    // with the default size - 2MB, it can support 50k blocks for shuffle data.
    long assumedFileSize =
        shuffleServer
            .getShuffleServerConf()
            .getLong(ShuffleServerConf.SERVER_SHUFFLE_INDEX_SIZE_HINT);
    if (shuffleServer.getShuffleBufferManager().requireReadMemory(assumedFileSize)) {
      ShuffleIndexResult shuffleIndexResult = null;
      try {
        final long start = System.currentTimeMillis();
        shuffleIndexResult =
            shuffleServer
                .getShuffleTaskManager()
                .getShuffleIndex(appId, shuffleId, partitionId, partitionNumPerRange, partitionNum);

        ManagedBuffer data = shuffleIndexResult.getManagedBuffer();
        ShuffleServerMetrics.counterTotalReadDataSize.inc(data.size());
        ShuffleServerMetrics.counterTotalReadLocalIndexFileSize.inc(data.size());
        response =
            new GetLocalShuffleIndexResponse(
                req.getRequestId(), status, msg, data, shuffleIndexResult.getDataFileLen());
        ReleaseMemoryAndRecordReadTimeListener listener =
            new ReleaseMemoryAndRecordReadTimeListener(
                start, assumedFileSize, data.size(), requestInfo, req, client);
        client.getChannel().writeAndFlush(response).addListener(listener);
        return;
      } catch (FileNotFoundException indexFileNotFoundException) {
        shuffleServer.getShuffleBufferManager().releaseReadMemory(assumedFileSize);
        if (shuffleIndexResult != null) {
          shuffleIndexResult.release();
        }
        LOG.warn(
            "Index file for {} is not found, maybe the data has been flushed to cold storage.",
            requestInfo,
            indexFileNotFoundException);
        response =
            new GetLocalShuffleIndexResponse(
                req.getRequestId(), status, msg, Unpooled.EMPTY_BUFFER, 0L);
      } catch (Exception e) {
        shuffleServer.getShuffleBufferManager().releaseReadMemory(assumedFileSize);
        if (shuffleIndexResult != null) {
          shuffleIndexResult.release();
        }
        status = StatusCode.INTERNAL_ERROR;
        msg = "Error happened when get shuffle index for " + requestInfo + ", " + e.getMessage();
        LOG.error(msg, e);
        response =
            new GetLocalShuffleIndexResponse(
                req.getRequestId(), status, msg, Unpooled.EMPTY_BUFFER, 0L);
      }
    } else {
      status = StatusCode.NO_BUFFER;
      msg = "Can't require memory to get shuffle index";
      LOG.error(msg + " for " + requestInfo);
      response =
          new GetLocalShuffleIndexResponse(
              req.getRequestId(), status, msg, Unpooled.EMPTY_BUFFER, 0L);
    }
    client.getChannel().writeAndFlush(response);
  }

  public void handleGetLocalShuffleData(TransportClient client, GetLocalShuffleDataRequest req) {
    String appId = req.getAppId();
    int shuffleId = req.getShuffleId();
    int partitionId = req.getPartitionId();
    int partitionNumPerRange = req.getPartitionNumPerRange();
    int partitionNum = req.getPartitionNum();
    long offset = req.getOffset();
    int length = req.getLength();
    long timestamp = req.getTimestamp();
    if (timestamp > 0) {
      long transportTime = System.currentTimeMillis() - timestamp;
      if (transportTime > 0) {
        shuffleServer
            .getNettyMetrics()
            .recordTransportTime(GetLocalShuffleDataRequest.class.getName(), transportTime);
      }
    }
    String storageType =
        shuffleServer.getShuffleServerConf().get(RssBaseConf.RSS_STORAGE_TYPE).name();
    StatusCode status = StatusCode.SUCCESS;
    String msg = "OK";
    GetLocalShuffleDataResponse response;
    String requestInfo =
        "appId["
            + appId
            + "], shuffleId["
            + shuffleId
            + "], partitionId["
            + partitionId
            + "], offset["
            + offset
            + "], length["
            + length
            + "]";

    int[] range =
        ShuffleStorageUtils.getPartitionRange(partitionId, partitionNumPerRange, partitionNum);
    Storage storage =
        shuffleServer
            .getStorageManager()
            .selectStorage(new ShuffleDataReadEvent(appId, shuffleId, partitionId, range[0]));
    if (storage != null) {
      storage.updateReadMetrics(new StorageReadMetrics(appId, shuffleId));
    }

    if (shuffleServer.getShuffleBufferManager().requireReadMemory(length)) {
      ShuffleDataResult sdr = null;
      try {
        final long start = System.currentTimeMillis();
        sdr =
            shuffleServer
                .getShuffleTaskManager()
                .getShuffleData(
                    appId,
                    shuffleId,
                    partitionId,
                    partitionNumPerRange,
                    partitionNum,
                    storageType,
                    offset,
                    length);
        ShuffleServerMetrics.counterTotalReadDataSize.inc(sdr.getDataLength());
        ShuffleServerMetrics.counterTotalReadLocalDataFileSize.inc(sdr.getDataLength());
        response =
            new GetLocalShuffleDataResponse(
                req.getRequestId(), status, msg, sdr.getManagedBuffer());
        ReleaseMemoryAndRecordReadTimeListener listener =
            new ReleaseMemoryAndRecordReadTimeListener(
                start, length, sdr.getDataLength(), requestInfo, req, client);
        client.getChannel().writeAndFlush(response).addListener(listener);
        return;
      } catch (Exception e) {
        shuffleServer.getShuffleBufferManager().releaseReadMemory(length);
        if (sdr != null) {
          sdr.release();
        }
        status = StatusCode.INTERNAL_ERROR;
        msg = "Error happened when get shuffle data for " + requestInfo + ", " + e.getMessage();
        LOG.error(msg, e);
        response =
            new GetLocalShuffleDataResponse(
                req.getRequestId(), status, msg, new NettyManagedBuffer(Unpooled.EMPTY_BUFFER));
      }
    } else {
      status = StatusCode.NO_BUFFER;
      msg = "Can't require memory to get shuffle data";
      LOG.error(msg + " for " + requestInfo);
      response =
          new GetLocalShuffleDataResponse(
              req.getRequestId(), status, msg, new NettyManagedBuffer(Unpooled.EMPTY_BUFFER));
    }
    client.getChannel().writeAndFlush(response);
  }

  private List<ShufflePartitionedData> toPartitionedData(SendShuffleDataRequest req) {
    List<ShufflePartitionedData> ret = Lists.newArrayList();

    for (Map.Entry<Integer, List<ShuffleBlockInfo>> entry : req.getPartitionToBlocks().entrySet()) {
      ret.add(new ShufflePartitionedData(entry.getKey(), toPartitionedBlock(entry.getValue())));
    }
    return ret;
  }

  private ShufflePartitionedBlock[] toPartitionedBlock(List<ShuffleBlockInfo> blocks) {
    if (blocks == null || blocks.size() == 0) {
      return new ShufflePartitionedBlock[] {};
    }
    ShufflePartitionedBlock[] ret = new ShufflePartitionedBlock[blocks.size()];
    int i = 0;
    for (ShuffleBlockInfo block : blocks) {
      ret[i] =
          new ShufflePartitionedBlock(
              block.getLength(),
              block.getUncompressLength(),
              block.getCrc(),
              block.getBlockId(),
              block.getTaskAttemptId(),
              block.getData());
      i++;
    }
    return ret;
  }

  class ReleaseMemoryAndRecordReadTimeListener implements ChannelFutureListener {
    private final long readStartedTime;
    private final long readBufferSize;
    private final long dataSize;
    private final String requestInfo;
    private final RequestMessage request;
    private final TransportClient client;

    ReleaseMemoryAndRecordReadTimeListener(
        long readStartedTime,
        long readBufferSize,
        long dataSize,
        String requestInfo,
        RequestMessage request,
        TransportClient client) {
      this.readStartedTime = readStartedTime;
      this.readBufferSize = readBufferSize;
      this.dataSize = dataSize;
      this.requestInfo = requestInfo;
      this.request = request;
      this.client = client;
    }

    @Override
    public void operationComplete(ChannelFuture future) {
      shuffleServer.getShuffleBufferManager().releaseReadMemory(readBufferSize);
      long readTime = System.currentTimeMillis() - readStartedTime;
      ShuffleServerMetrics.counterTotalReadTime.inc(readTime);
      shuffleServer.getNettyMetrics().recordProcessTime(request.getClass().getName(), readTime);
      if (!future.isSuccess()) {
        Throwable cause = future.cause();
        String errorMsg =
            "Error happened when executing "
                + request.getOperationType()
                + " for "
                + requestInfo
                + ", "
                + cause.getMessage();
        if (future.channel().isWritable()) {
          RpcResponse errorResponse;
          if (request instanceof GetLocalShuffleDataRequest) {
            errorResponse =
                new GetLocalShuffleDataResponse(
                    request.getRequestId(),
                    StatusCode.INTERNAL_ERROR,
                    errorMsg,
                    new NettyManagedBuffer(Unpooled.EMPTY_BUFFER));
          } else if (request instanceof GetLocalShuffleIndexRequest) {
            errorResponse =
                new GetLocalShuffleIndexResponse(
                    request.getRequestId(),
                    StatusCode.INTERNAL_ERROR,
                    errorMsg,
                    Unpooled.EMPTY_BUFFER,
                    0L);
          } else if (request instanceof GetMemoryShuffleDataRequest) {
            errorResponse =
                new GetMemoryShuffleDataResponse(
                    request.getRequestId(),
                    StatusCode.INTERNAL_ERROR,
                    errorMsg,
                    Lists.newArrayList(),
                    Unpooled.EMPTY_BUFFER);
          } else {
            LOG.error("Cannot handle request {}", request.type(), cause);
            return;
          }
          client.getChannel().writeAndFlush(errorResponse);
        }
        LOG.error(
            "Failed to execute {} for {}. Took {} ms and could not retrieve {} bytes of data",
            request.getOperationType(),
            requestInfo,
            readTime,
            dataSize,
            cause);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Successfully executed {} for {}. Took {} ms and retrieved {} bytes of data",
              request.getOperationType(),
              requestInfo,
              readTime,
              dataSize);
        }
      }
    }
  }
}
