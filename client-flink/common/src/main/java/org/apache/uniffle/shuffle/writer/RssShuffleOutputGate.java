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

package org.apache.uniffle.shuffle.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.util.function.SupplierWithException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.response.SendShuffleDataResult;
import org.apache.uniffle.client.util.ClientUtils;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.ChecksumUtils;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.shuffle.RssFlinkConfig;
import org.apache.uniffle.shuffle.RssShuffleDescriptor;
import org.apache.uniffle.shuffle.buffer.WriteBufferPacker;
import org.apache.uniffle.shuffle.utils.BufferUtils;
import org.apache.uniffle.shuffle.utils.CommonUtils;
import org.apache.uniffle.shuffle.utils.ExceptionUtils;
import org.apache.uniffle.shuffle.utils.FlinkShuffleUtils;

public class RssShuffleOutputGate {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleOutputGate.class);

  private final RssShuffleDescriptor shuffleDesc;

  private int numSubs;
  private ShuffleWriteClient shuffleWriteClient;
  protected final SupplierWithException<BufferPool, IOException> bufferPoolFactory;
  protected BufferPool bufferPool;
  protected int bufferSize;

  private int shuffleId;
  private int attemptId;
  private int partitionId;

  private final int bitmapSplitNum;

  private final WriteBufferPacker writeBufferPacker;

  private final Object lock = new Object();

  private Map<Integer, Integer> partitionToSeqNo = Maps.newHashMap();
  private Set<ShuffleServerInfo> shuffleServerInfos;
  private final Set<Long> successBlockIds;
  private final Set<Long> failedBlockIds;
  private final Map<Integer, List<Long>> partitionToBlocks = JavaUtils.newConcurrentMap();
  Map<Integer, List<ShuffleServerInfo>> partitionToServers;

  private final int numMappers;

  public RssShuffleOutputGate(
      RssShuffleDescriptor pShuffleDesc,
      int pNumSubs,
      int pBufferSize,
      SupplierWithException<BufferPool, IOException> pBufferPoolFactory,
      Configuration pConfig,
      int pNumMappers) {

    this.shuffleDesc = pShuffleDesc;
    this.numSubs = pNumSubs;
    this.bufferPoolFactory = pBufferPoolFactory;
    this.bufferSize = pBufferSize;
    this.shuffleWriteClient = FlinkShuffleUtils.createShuffleClient(pConfig);

    this.shuffleId = shuffleDesc.getShuffleResource().getShuffleResourceDescriptor().getShuffleId();
    this.attemptId = shuffleDesc.getShuffleResource().getShuffleResourceDescriptor().getAttemptId();
    this.partitionId =
        shuffleDesc.getShuffleResource().getShuffleResourceDescriptor().getPartitionId();

    this.bitmapSplitNum = pConfig.getInteger(RssFlinkConfig.RSS_CLIENT_BITMAP_SPLIT_NUM);
    this.writeBufferPacker = new WriteBufferPacker(this::write);

    this.successBlockIds = new HashSet<>();
    this.failedBlockIds = new HashSet<>();
    this.partitionToServers = new HashMap<>();
    this.partitionToServers = shuffleDesc.getShuffleResource().getPartitionToServers();
    this.numMappers = pNumMappers;

    this.shuffleServerInfos = new HashSet<>();
    this.shuffleServerInfos.addAll(shuffleDesc.getShuffleResource().getMapPartitionLocation());
  }

  public void setup() throws IOException, InterruptedException {
    bufferPool = CommonUtils.checkNotNull(bufferPoolFactory.get());
    CommonUtils.checkArgument(
        bufferPool.getNumberOfRequiredMemorySegments() >= 2,
        "Too few buffers for transfer, the minimum valid required size is 2.");

    // guarantee that we have at least one buffer
    BufferUtils.bufferReservationForRequirements(bufferPool, 1);
  }

  public void write(Buffer buffer, int subIdx) throws InterruptedException {
    writeBufferPacker.process(buffer, subIdx);
  }

  public void write(ByteBuf byteBuf, int subIdx) throws InterruptedException {
    synchronized (lock) {
      List<ShuffleBlockInfo> sentBlocks = new ArrayList<>();
      ShuffleBlockInfo shuffleBlock = createShuffleBlock(byteBuf, subIdx);
      partitionToBlocks.computeIfAbsent(shuffleBlock.getPartitionId(), key -> Lists.newArrayList());
      partitionToBlocks.get(shuffleBlock.getPartitionId()).add(shuffleBlock.getBlockId());
      sentBlocks.add(shuffleBlock);
      JobID jobID = shuffleDesc.getJobId();
      SendShuffleDataResult result =
          shuffleWriteClient.sendShuffleData(jobID.toString(), sentBlocks, () -> false);
      successBlockIds.addAll(result.getSuccessBlockIds());
      failedBlockIds.addAll(result.getFailedBlockIds());
    }
  }

  protected ShuffleBlockInfo createShuffleBlock(ByteBuf byteBuf, int subIdx) {
    byte[] data = new byte[byteBuf.readableBytes()];
    final int uncompressLength = data.length;
    byte[] compressed = data;
    final long crc32 = ChecksumUtils.getCrc32(compressed);
    final long blockId = ClientUtils.getBlockId(partitionId, attemptId, getNextSeqNo(subIdx));
    List<ShuffleServerInfo> shuffleServerInfos = partitionToServers.get(subIdx);
    return new ShuffleBlockInfo(
        shuffleId,
        partitionId,
        blockId,
        compressed.length,
        crc32,
        compressed,
        shuffleServerInfos,
        uncompressLength,
        byteBuf.readableBytes(),
        attemptId);
  }

  private int getNextSeqNo(int partitionId) {
    partitionToSeqNo.putIfAbsent(partitionId, 0);
    int seqNo = partitionToSeqNo.get(partitionId);
    partitionToSeqNo.put(partitionId, seqNo + 1);
    return seqNo;
  }

  public void regionStart(boolean isBroadcast) {
    LOG.info("regionStart, isBroadcast = {}", isBroadcast);
  }

  public void regionFinish() throws InterruptedException {
    writeBufferPacker.drain();
    JobID jobID = shuffleDesc.getJobId();
    shuffleWriteClient.reportShuffleResult(
        partitionToServers, jobID.toString(), 0, attemptId, partitionToBlocks, bitmapSplitNum);
    LOG.info("regionFinish.");
  }

  public BufferPool getBufferPool() {
    return bufferPool;
  }

  public void finish() throws InterruptedException {
    sendCommit();
  }

  public void close() throws IOException {
    Throwable closeException = null;
    try {
      if (bufferPool != null) {
        bufferPool.lazyDestroy();
      }
    } catch (Throwable throwable) {
      closeException = throwable;
      LOG.error("Failed to close local buffer pool.", throwable);
    }

    try {
      writeBufferPacker.close();
    } catch (Throwable throwable) {
      closeException = closeException == null ? throwable : closeException;
      LOG.error("Failed to close buffer packer.", throwable);
    }

    try {
      shuffleWriteClient.close();
    } catch (Throwable throwable) {
      closeException = closeException == null ? throwable : closeException;
      LOG.error("Failed to close shuffle write client.", throwable);
    }

    if (closeException != null) {
      ExceptionUtils.translateToRuntimeException(closeException);
    }
  }

  protected void sendCommit() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    JobID jobID = shuffleDesc.getJobId();
    Future<Boolean> future =
        executor.submit(
            () ->
                shuffleWriteClient.sendCommit(
                    shuffleServerInfos, jobID.toString(), shuffleId, numMappers));
    int maxWait = 5000;
    int currentWait = 200;
    long start = System.currentTimeMillis();
    while (!future.isDone()) {
      LOG.info(
          "Wait commit to shuffle server for task["
              + attemptId
              + "] cost "
              + (System.currentTimeMillis() - start)
              + " ms");
      Uninterruptibles.sleepUninterruptibly(currentWait, TimeUnit.MILLISECONDS);
      currentWait = Math.min(currentWait * 2, maxWait);
    }
    try {
      if (!future.get()) {
        throw new RssException("Failed to commit task to shuffle server");
      }
    } catch (InterruptedException ie) {
      LOG.warn("Ignore the InterruptedException which should be caused by internal killed");
    } catch (Exception e) {
      throw new RssException("Exception happened when get commit status", e);
    } finally {
      executor.shutdown();
    }
  }
}
