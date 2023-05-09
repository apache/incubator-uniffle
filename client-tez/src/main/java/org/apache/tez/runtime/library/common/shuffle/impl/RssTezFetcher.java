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

package org.apache.tez.runtime.library.common.shuffle.impl;

import java.io.*;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.*;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.RssTezBypassWriter;
import org.apache.uniffle.client.api.ShuffleReadClient;
import org.apache.uniffle.client.response.CompressedShuffleBlock;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.compression.Codec;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.ByteUnit;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RssTezFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleManager.class);
  private final FetcherCallback fetcherCallback;

  private enum ShuffleErrors {
    IO_ERROR, WRONG_LENGTH, BAD_ID, WRONG_MAP,
    CONNECTION, WRONG_REDUCE
  }

  private static final String SHUFFLE_ERR_GRP_NAME = "Shuffle Errors";
  private static final double BYTES_PER_MILLIS_TO_MBS = 1000d / (ByteUnit.MiB.toBytes(1));
  private final DecimalFormat mbpsFormat = new DecimalFormat("0.00");

  private final Configuration conf;
  private final FetchedInputAllocator inputManager;

  private long totalBlockCount;
  private long copyBlockCount = 0;

  private volatile boolean stopped = false;

  private final ShuffleReadClient shuffleReadClient;
  Map<Integer, Roaring64NavigableMap> rssSuccessBlockIdBitmapMap;
  private int partition;
  private long readTime = 0;
  private long decompressTime = 0;
  private long serializeTime = 0;
  private long waitTime = 0;
  private long copyTime = 0;  // the sum of readTime + decompressTime + serializeTime + waitTime
  private long unCompressionLength = 0;
  private final TaskAttemptID reduceId = null; //  todo： 编译通过
  private int uniqueMapId = 0;

  private boolean hasPendingData = false;
  private long startWait;
  private int waitCount = 0;
  private byte[] uncompressedData = null;
  private RssConf rssConf;
  private Codec codec;
  private CompressionCodec tezCodec;
  private Set<ShuffleServerInfo> serverInfoSet;

  RssTezFetcher(FetcherCallback fetcherCallback, Configuration conf,
                FetchedInputAllocator inputManager,
                ShuffleReadClient shuffleReadClient,
                Map<Integer, Roaring64NavigableMap> rssSuccessBlockIdBitmapMap,
                int partition, // 本task要拉取的partition
                long totalBlockCount, CompressionCodec tezCodec,
                RssConf rssConf, Set<ShuffleServerInfo> serverInfoSet) {
    this.fetcherCallback = fetcherCallback;
    this.conf = conf;
    this.inputManager = inputManager;
    this.shuffleReadClient = shuffleReadClient;
    this.rssSuccessBlockIdBitmapMap = rssSuccessBlockIdBitmapMap;
    this.partition = partition;
    this.totalBlockCount = totalBlockCount;
    this.rssConf = rssConf;
    this.codec = Codec.newInstance(rssConf);
    this.tezCodec = tezCodec;
    this.serverInfoSet = serverInfoSet;
  }

  public void fetchAllRssBlocks() throws IOException {
    while (!stopped) {
      try {
        copyFromRssServer();
      } catch (Exception e) {
        LOG.error("Failed to fetchAllRssBlocks.", e);
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
      codec.decompress(compressedData, uncompressedLen, decompressedBuffer, 0);
      uncompressedData = decompressedBuffer.array();
      unCompressionLength += compressedBlock.getUncompressLength();
      long decompressDuration = System.currentTimeMillis() - startDecompress;
      decompressTime += decompressDuration;
    }

    if (uncompressedData != null) {
      // start to merge
      final long startSerialization = System.currentTimeMillis();
      int compressedDataLength = compressedData != null ? compressedData.capacity() : 0;
      if (issueMapOutputMerge(compressedDataLength)) {
        long serializationDuration = System.currentTimeMillis() - startSerialization;
        serializeTime += serializationDuration;
        // if reserve successes, reset status for next fetch
        if (hasPendingData) {
          waitTime += System.currentTimeMillis() - startWait;
        }
        hasPendingData = false;
        uncompressedData = null;
      } else {
        LOG.info("uncompressedData is null");
        // if reserve fail, return and wait
        startWait = System.currentTimeMillis();
        return;
      }

      // update some status
      copyBlockCount++;
      copyTime = readTime + decompressTime + serializeTime + waitTime;
    } else {
      LOG.info("uncompressedData is null");
      // finish reading data, close related reader and check data consistent
      shuffleReadClient.close();
      shuffleReadClient.checkProcessedBlockIds();
      shuffleReadClient.logStatics();
      LOG.info("reduce task partition:" + partition + " cost " + readTime + " ms to fetch and "
          + decompressTime + " ms to decompress with unCompressionLength["
          + unCompressionLength + "] and " + serializeTime + " ms to serialize and "
          + waitTime + " ms to wait resource");
      LOG.info("Stop fetcher");
      stopFetch();
    }
  }

  private boolean issueMapOutputMerge(int compressedLength) throws IOException {
    // Allocate a MapOutput (either in-memory or on-disk) to put uncompressed block
    // In Rss, a MapOutput is sent as multiple blocks, so the reducer needs to
    // treat each "block" as a faked "mapout".
    // To avoid name conflicts, we use getNextUniqueTaskAttemptID instead.
    // It will generate a unique TaskAttemptID(increased_seq++, 0).
    InputAttemptIdentifier uniqueInputAttemptIdentifier = getNextUniqueInputAttemptIdentifier();
    FetchedInput fetchedInput = null;

    try {
      LOG.info("IssueMapOutputMerge, uncompressedData:{}", uncompressedData.length);
      fetchedInput = inputManager.allocate(uncompressedData.length,
          compressedLength, uniqueInputAttemptIdentifier);
      LOG.info("IssueMapOutputMerge uncompressedData length:{}", uncompressedData.length);
      // 将数据放到对象的字节数组中。
    } catch (IOException ioe) {
      // kill this reduce attempt
      throw ioe;
    }

    // write data to mapOutput
    try {  // 分配到空间，并将数据写入到分配空间 ✨
      RssTezBypassWriter.write(fetchedInput, uncompressedData);
      // let the merger knows this block is ready for merging
      fetcherCallback.fetchSucceeded(null, uniqueInputAttemptIdentifier, fetchedInput,
          compressedLength, unCompressionLength, -1);
    } catch (Throwable t) {
      LOG.error("Failed to write fetchedInput.", t);
      throw new RssException("Partition: " + partition + " cannot write block to "
          + fetchedInput.getClass().getSimpleName() + " due to: " + t.getClass().getName());
    }
    return true;
  }


  private InputAttemptIdentifier getNextUniqueInputAttemptIdentifier() {
    InputAttemptIdentifier nextAttemptId = new InputAttemptIdentifier(uniqueMapId++, 0);
    return nextAttemptId;
  }

  private void stopFetch() {
    stopped = true;
  }


  @VisibleForTesting
  public int getRetryCount() {
    return waitCount;
  }


  private static class MapOutputStat {
    final InputAttemptIdentifier srcAttemptId;
    final long decompressedLength;
    final long compressedLength;
    final int forReduce;

    MapOutputStat(InputAttemptIdentifier srcAttemptId, long decompressedLength, long compressedLength, int forReduce) {
      this.srcAttemptId = srcAttemptId;
      this.decompressedLength = decompressedLength;
      this.compressedLength = compressedLength;
      this.forReduce = forReduce;
    }

    @Override
    public String toString() {
      return "id: " + srcAttemptId + ", decompressed length: " + decompressedLength + ", compressed length: " + compressedLength + ", reduce: " + forReduce;
    }
  }


}
