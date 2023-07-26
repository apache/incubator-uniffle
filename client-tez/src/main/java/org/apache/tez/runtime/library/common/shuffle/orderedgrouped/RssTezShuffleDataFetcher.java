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

package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleReadClient;
import org.apache.uniffle.client.response.CompressedShuffleBlock;
import org.apache.uniffle.common.compression.Codec;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;

public class RssTezShuffleDataFetcher extends CallableWithNdc<Void> {
  private static final Logger LOG = LoggerFactory.getLogger(RssTezShuffleDataFetcher.class);

  private enum ShuffleErrors {
    IO_ERROR,
    WRONG_LENGTH,
    BAD_ID,
    WRONG_MAP,
    CONNECTION,
    WRONG_REDUCE
  }

  private static final String SHUFFLE_ERR_GRP_NAME = "Shuffle Errors";

  private final TezCounter ioErrs;
  private final MergeManager merger;
  private final long totalBlockCount;

  private long copyBlockCount = 0;
  private volatile boolean stopped = false;

  private final ShuffleReadClient shuffleReadClient;
  private long readTime = 0;
  private long decompressTime = 0;
  private long serializeTime = 0;
  private long waitTime = 0;
  private long copyTime = 0; // the sum of readTime + decompressTime + serializeTime + waitTime
  private long unCompressionLength = 0;
  private final InputAttemptIdentifier inputAttemptIdentifier;
  private int uniqueMapId = 0;

  private boolean hasPendingData = false;
  private long startWait;
  private int waitCount = 0;
  private byte[] uncompressedData = null;
  private final Codec rssCodec;
  private Integer partitionId;
  private final ExceptionReporter exceptionReporter;

  private final AtomicInteger issuedCnt = new AtomicInteger(0);

  public RssTezShuffleDataFetcher(
      InputAttemptIdentifier inputAttemptIdentifier,
      Integer partitionId,
      MergeManager merger,
      TezCounters tezCounters,
      ShuffleReadClient shuffleReadClient,
      long totalBlockCount,
      RssConf rssConf,
      ExceptionReporter exceptionReporter) {
    this.merger = merger;
    this.partitionId = partitionId;
    this.inputAttemptIdentifier = inputAttemptIdentifier;
    this.exceptionReporter = exceptionReporter;
    ioErrs =
        tezCounters.findCounter(
            SHUFFLE_ERR_GRP_NAME, RssTezShuffleDataFetcher.ShuffleErrors.IO_ERROR.toString());
    this.shuffleReadClient = shuffleReadClient;
    this.totalBlockCount = totalBlockCount;

    this.rssCodec = Codec.newInstance(rssConf);
    LOG.info(
        "RssTezShuffleDataFetcher, partitionId:{}, inputAttemptIdentifier:{}.",
        this.partitionId,
        this.inputAttemptIdentifier);
  }

  @Override
  public Void callInternal() {
    try {
      fetchAllRssBlocks();
    } catch (InterruptedException ie) {
      // might not be respected when fetcher is in progress / server is busy.  TEZ-711
      // Set the status back
      LOG.warn(ie.getMessage(), ie);
      Thread.currentThread().interrupt();
      return null;
    } catch (Throwable t) {
      LOG.warn(t.getMessage(), t);
      exceptionReporter.reportException(t);
      // Shuffle knows how to deal with failures post shutdown via the onFailure hook
    }
    return null;
  }

  public void fetchAllRssBlocks() throws IOException, InterruptedException {
    while (!stopped) {
      try {
        // If merge is on, block
        merger.waitForInMemoryMerge();
        // Do shuffle
        copyFromRssServer();
      } catch (Exception e) {
        LOG.warn(e.getMessage(), e);
        throw e;
      }
    }
  }

  @VisibleForTesting
  public void copyFromRssServer() throws IOException {
    CompressedShuffleBlock compressedBlock = null;
    ByteBuffer compressedData = null;
    // fetch a block
    if (!hasPendingData) {
      final long startFetch = System.currentTimeMillis();
      compressedBlock = shuffleReadClient.readShuffleBlockData();
      if (compressedBlock != null) {
        compressedData = compressedBlock.getByteBuffer();
      }
      long fetchDuration = System.currentTimeMillis() - startFetch;
      readTime += fetchDuration;
    }

    // uncompress the block
    if (!hasPendingData && compressedData != null) {
      final long startDecompress = System.currentTimeMillis();
      int uncompressedLen = compressedBlock.getUncompressLength();
      ByteBuffer decompressedBuffer = ByteBuffer.allocate(uncompressedLen);
      rssCodec.decompress(compressedData, uncompressedLen, decompressedBuffer, 0);
      uncompressedData = decompressedBuffer.array();
      unCompressionLength += compressedBlock.getUncompressLength();
      long decompressDuration = System.currentTimeMillis() - startDecompress;
      decompressTime += decompressDuration;
    }

    if (uncompressedData != null) {
      // start to merge
      final long startSerialization = System.currentTimeMillis();
      if (issueMapOutputMerge()) {
        long serializationDuration = System.currentTimeMillis() - startSerialization;
        serializeTime += serializationDuration;
        // if reserve successes, reset status for next fetch
        if (hasPendingData) {
          waitTime += System.currentTimeMillis() - startWait;
        }
        hasPendingData = false;
        uncompressedData = null;
      } else {
        // if reserve fail, return and wait
        startWait = System.currentTimeMillis();
        return;
      }

      // update some status
      copyBlockCount++;
      copyTime = readTime + decompressTime + serializeTime + waitTime;
      updateStatus();
    } else {
      // finish reading data, close related reader and check data consistent
      shuffleReadClient.close();
      shuffleReadClient.checkProcessedBlockIds();
      shuffleReadClient.logStatics();
      LOG.info(
          "Reduce task "
              + inputAttemptIdentifier
              + " read block cnt: "
              + copyBlockCount
              + " cost "
              + readTime
              + " ms to fetch and "
              + decompressTime
              + " ms to decompress with unCompressionLength["
              + unCompressionLength
              + "] and "
              + serializeTime
              + " ms to serialize and "
              + waitTime
              + " ms to wait resource"
              + ", copy time:"
              + copyTime);
      stopFetch();
    }
  }

  public Integer getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(Integer partitionId) {
    this.partitionId = partitionId;
  }

  private boolean issueMapOutputMerge() throws IOException {
    // Allocate a MapOutput (either in-memory or on-disk) to put uncompressed block
    // In Rss, a MapOutput is sent as multiple blocks, so the reducer needs to
    // treat each "block" as a faked "mapout".
    // To avoid name conflicts, we use getNextUniqueTaskAttemptID instead.
    // It will generate a unique TaskAttemptID(increased_seq++, 0).
    InputAttemptIdentifier uniqueInputAttemptIdentifier = getNextUniqueInputAttemptIdentifier();
    MapOutput mapOutput = null;
    try {
      issuedCnt.incrementAndGet();
      LOG.info(
          "IssueMapOutputMerge, uncompressedData length:{}, issueCnt:{}, totalBlockCount:{}",
          uncompressedData.length,
          issuedCnt.get(),
          totalBlockCount);
      mapOutput = merger.reserve(uniqueInputAttemptIdentifier, uncompressedData.length, 0, 1);
    } catch (IOException ioe) {
      // kill this reduce attempt
      ioErrs.increment(1);
      throw ioe;
    }
    // Check if we can shuffle *now* ...
    if (mapOutput == null) {
      LOG.info("RssMRFetcher" + " - MergeManager returned status WAIT ...");
      // Not an error but wait to process data.
      // Use a retry flag to avoid re-fetch and re-uncompress.
      hasPendingData = true;
      waitCount++;
      return false;
    }

    // write data to mapOutput
    try {
      RssTezBypassWriter.write(mapOutput, uncompressedData);
      // let the merger knows this block is ready for merging
      mapOutput.commit();
    } catch (Throwable t) {
      ioErrs.increment(1);
      mapOutput.abort();
      throw new RssException(
          "Reduce: "
              + inputAttemptIdentifier
              + " cannot write block to "
              + mapOutput.getClass().getSimpleName()
              + " due to: "
              + t.getClass().getName());
    }
    return true;
  }

  private InputAttemptIdentifier getNextUniqueInputAttemptIdentifier() {
    return new InputAttemptIdentifier(uniqueMapId++, 0);
  }

  private void updateStatus() {}

  @VisibleForTesting
  public int getRetryCount() {
    return waitCount;
  }

  private void stopFetch() {
    LOG.info("RssTezShuffleDataFetcher stop fetch");
    stopped = true;
  }

  public void shutDown() {
    stopFetch();
    LOG.info("RssTezShuffleDataFetcher shutdown");
  }
}
