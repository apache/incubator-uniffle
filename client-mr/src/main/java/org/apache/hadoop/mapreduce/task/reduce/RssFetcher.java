/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.Progress;

import com.tencent.rss.client.api.ShuffleReadClient;
import com.tencent.rss.client.response.CompressedShuffleBlock;
import com.tencent.rss.common.RssShuffleUtils;
import com.tencent.rss.common.exception.RssException;
import com.tencent.rss.common.util.ByteUnit;

public class RssFetcher<K,V> {

  private static final Log LOG = LogFactory.getLog(RssFetcher.class);

  private final Reporter reporter;

  private enum ShuffleErrors {
    IO_ERROR, WRONG_LENGTH, BAD_ID, WRONG_MAP,
    CONNECTION, WRONG_REDUCE
  }

  private static final String SHUFFLE_ERR_GRP_NAME = "Shuffle Errors";
  private static final double BYTES_PER_MILLIS_TO_MBS = 1000d / (ByteUnit.MiB.toBytes(1));
  private final DecimalFormat mbpsFormat = new DecimalFormat("0.00");

  private final JobConf jobConf;
  private final Counters.Counter connectionErrs;
  private final Counters.Counter ioErrs;
  private final Counters.Counter wrongLengthErrs;
  private final Counters.Counter badIdErrs;
  private final Counters.Counter wrongMapErrs;
  private final Counters.Counter wrongReduceErrs;
  private final TaskStatus status;
  private final MergeManager<K,V> merger;
  private final Progress progress;
  private final ShuffleClientMetrics metrics;
  private long totalBlockCount;
  private long copyBlockCount = 0;

  private volatile boolean stopped = false;

  private ShuffleReadClient shuffleReadClient;
  private long readTime = 0;
  private long decompressTime = 0;
  private long serializeTime = 0;
  private long copyTime = 0;  // the sum of readTime + decompressTime + serializeTime
  private long unCompressionLength = 0;
  private final TaskAttemptID reduceId;
  private int uniqueMapId = 0;

  RssFetcher(JobConf job, TaskAttemptID reduceId,
             TaskStatus status,
             MergeManager<K,V> merger,
             Progress progress,
             Reporter reporter, ShuffleClientMetrics metrics,
             ShuffleReadClient shuffleReadClient,
             long totalBlockCount) {
    this.jobConf = job;
    this.reporter = reporter;
    this.status = status;
    this.merger = merger;
    this.progress = progress;
    this.metrics = metrics;
    this.reduceId = reduceId;
    ioErrs = reporter.getCounter(SHUFFLE_ERR_GRP_NAME,
      RssFetcher.ShuffleErrors.IO_ERROR.toString());
    wrongLengthErrs = reporter.getCounter(SHUFFLE_ERR_GRP_NAME,
      RssFetcher.ShuffleErrors.WRONG_LENGTH.toString());
    badIdErrs = reporter.getCounter(SHUFFLE_ERR_GRP_NAME,
      RssFetcher.ShuffleErrors.BAD_ID.toString());
    wrongMapErrs = reporter.getCounter(SHUFFLE_ERR_GRP_NAME,
      RssFetcher.ShuffleErrors.WRONG_MAP.toString());
    connectionErrs = reporter.getCounter(SHUFFLE_ERR_GRP_NAME,
      RssFetcher.ShuffleErrors.CONNECTION.toString());
    wrongReduceErrs = reporter.getCounter(SHUFFLE_ERR_GRP_NAME,
      RssFetcher.ShuffleErrors.WRONG_REDUCE.toString());

    this.shuffleReadClient = shuffleReadClient;
    this.totalBlockCount = totalBlockCount;
  }

  public void fetchAllRssBlocks() throws IOException, InterruptedException {
    while (!stopped) {
      try {
        // If merge is on, block
        merger.waitForResource();
        // Do shuffle
        metrics.threadBusy();
        copyFromRssServer();
      } catch (Exception e) {
        throw e;
      } finally {
        metrics.threadFree();
      }
    }
  }

  @VisibleForTesting
  public void copyFromRssServer() throws IOException {
    // fetch a block
    final long startFetch = System.currentTimeMillis();
    CompressedShuffleBlock compressedBlock = shuffleReadClient.readShuffleBlockData();
    ByteBuffer compressedData = null;
    if (compressedBlock != null) {
      compressedData = compressedBlock.getByteBuffer();
    }
    long fetchDuration = System.currentTimeMillis() - startFetch;
    readTime += fetchDuration;

    // uncompress the block
    if (compressedData != null) {
      final long startDecompress = System.currentTimeMillis();
      byte[] uncompressedData = RssShuffleUtils.decompressData(
        compressedData, compressedBlock.getUncompressLength(), false).array();
      unCompressionLength += compressedBlock.getUncompressLength();
      long decompressDuration = System.currentTimeMillis() - startDecompress;
      decompressTime += decompressDuration;

      // Allocate a MapOutput (either in-memory or on-disk) to put uncompressed block
      // In Rss, a MapOutput is sent as multiple blocks, so the reducer needs to
      // treat each "block" as a faked "mapout".
      // To avoid name conflicts, we use getNextUniqueTaskAttemptID instead.
      // It will generate a unique TaskAttemptID(increased_seq++, 0).
      final long startSerialization = System.currentTimeMillis();
      TaskAttemptID mapId = getNextUniqueTaskAttemptID();
      MapOutput<K, V> mapOutput = null;
      try {
        mapOutput = merger.reserve(mapId, compressedBlock.getUncompressLength(), 0);
      } catch (IOException ioe) {
        // kill this reduce attempt
        ioErrs.increment(1);
        throw ioe;
      }
      // Check if we can shuffle *now* ...
      if (mapOutput == null) {
        LOG.info("RssMRFetcher" + " - MergeManager returned status WAIT ...");
        //Not an error but wait to process data.
        return;
      }

      // write data to mapOutput
      try {
        RssBypassWriter.write(mapOutput, uncompressedData);
        // let the merger knows this block is ready for merging
        mapOutput.commit();
        if (mapOutput instanceof OnDiskMapOutput) {
          LOG.info("Reduce: " + reduceId + " allocates disk to accept block "
            + " with byte sizes: " + uncompressedData.length);
        }
      } catch (Throwable t) {
        ioErrs.increment(1);
        mapOutput.abort();
        throw new RssException("Reduce: " + reduceId + " cannot write block to "
          + mapOutput.getClass().getSimpleName() + " due to: " + t.getClass().getName());
      }
      long serializationDuration = System.currentTimeMillis() - startSerialization;
      serializeTime += serializationDuration;

      // update some status
      copyBlockCount++;
      copyTime = readTime + decompressTime + serializeTime;
      updateStatus();
      reporter.progress();
    } else {
      // finish reading data, close related reader and check data consistent
      shuffleReadClient.close();
      shuffleReadClient.checkProcessedBlockIds();
      shuffleReadClient.logStatics();
      metrics.inputBytes(unCompressionLength);
      LOG.info("reduce task " + reduceId.toString() + " cost " + readTime + " ms to fetch and "
        + decompressTime + " ms to decompress with unCompressionLength["
        + unCompressionLength + "] and " + serializeTime + " ms to serialize");
      stopFetch();
    }
  }

  private TaskAttemptID getNextUniqueTaskAttemptID() {
    TaskID taskID = new TaskID(reduceId.getJobID(), TaskType.MAP, uniqueMapId++);
    return new TaskAttemptID(taskID, 0);
  }

  private void stopFetch() {
    stopped = true;
  }

  private void updateStatus() {
    progress.set((float) copyBlockCount / totalBlockCount);
    String statusString = copyBlockCount + " / " + totalBlockCount + " copied.";
    status.setStateString(statusString);

    if (copyTime == 0) {
      copyTime = 1;
    }
    double bytesPerMillis = (double) unCompressionLength / copyTime;
    double transferRate = bytesPerMillis * BYTES_PER_MILLIS_TO_MBS;

    progress.setStatus("copy(" + copyBlockCount + " of " + totalBlockCount + " at "
      + mbpsFormat.format(transferRate) + " MB/s)");
  }

}
