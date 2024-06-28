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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FileChunk;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.combine.Combiner;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Writer;
import org.apache.tez.runtime.library.common.sort.impl.TezMerger;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RssInMemoryMerger extends MergeThread<MapOutput> {

  private static final Logger LOG = LoggerFactory.getLogger(RssInMemoryMerger.class);

  /*
   * Spill path must be unique, is composed of unique_id, src_id and spill_id.
   * unique_id is task attempt id + io context increasing id.
   * src_id the source input id, is determined by partition and source task.
   * spill_id is the spill id of map task.
   * */
  private static final String SPILL_FILE_PATTERN = "%s/%s_src_%d_spill_%d.out";

  private final InputContext inputContext;
  private final Configuration conf;
  private final RssMergeManager manager;
  private final CompressionCodec codec;
  private final Combiner combiner;

  private final FileSystem remoteFs;
  private final Path spillPath;
  private final String appAttemptId;
  private volatile InputAttemptIdentifier srcTaskIdentifier;
  private volatile Path outputPath;

  private final TezCounter spilledRecordsCounter;
  private final TezCounter numMemToRemoteMerges;
  private final TezCounter additionalBytesRead;
  private final TezCounter additionalBytesWritten;

  public enum Counter {
    NUM_MEM_TO_REMOTE_MERGES
  }

  public RssInMemoryMerger(
      RssMergeManager manager,
      Configuration conf,
      InputContext inputContext,
      Combiner combiner,
      ExceptionReporter reporter,
      CompressionCodec codec,
      FileSystem remoteFs,
      String spillPath,
      String appAttemptId) {
    super(manager, Integer.MAX_VALUE, reporter);
    this.setName(
        "MemtoRemoteMerger ["
            + TezUtilsInternal.cleanVertexName(inputContext.getSourceVertexName())
            + "]");
    this.setDaemon(true);
    this.manager = manager;
    this.inputContext = inputContext;
    this.conf = conf;
    this.remoteFs = remoteFs;
    this.spillPath = new Path(spillPath);
    this.appAttemptId = appAttemptId;
    this.codec = codec;
    this.combiner = combiner;
    this.spilledRecordsCounter =
        inputContext.getCounters().findCounter(TaskCounter.SPILLED_RECORDS);
    this.numMemToRemoteMerges =
        inputContext.getCounters().findCounter(Counter.NUM_MEM_TO_REMOTE_MERGES);
    this.additionalBytesRead =
        inputContext.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILLS_BYTES_READ);
    this.additionalBytesWritten =
        inputContext.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILLS_BYTES_WRITTEN);
  }

  @Override
  public void merge(List<MapOutput> inputs) throws IOException, InterruptedException {
    if (inputs == null || inputs.size() == 0) {
      return;
    }
    numMemToRemoteMerges.increment(1);
    inputContext.notifyProgress();

    this.srcTaskIdentifier = inputs.get(0).getAttemptIdentifier();
    List<TezMerger.Segment> inMemorySegments = new ArrayList();

    manager.createInMemorySegments(inputs, inMemorySegments, 0);
    int noInMemorySegments = inMemorySegments.size();

    String mergedFile =
        String.format(
            SPILL_FILE_PATTERN,
            appAttemptId,
            inputContext.getUniqueIdentifier(),
            srcTaskIdentifier.getInputIdentifier(),
            srcTaskIdentifier.getSpillEventId());
    this.outputPath = new Path(spillPath, mergedFile);

    Writer writer = null;
    long outFileLen = 0;
    try {
      writer =
          new Writer(
              conf,
              remoteFs,
              outputPath,
              ConfigUtils.getIntermediateInputKeyClass(conf),
              ConfigUtils.getIntermediateInputValueClass(conf),
              codec,
              null,
              null);

      TezRawKeyValueIterator rIter;
      LOG.info("Initiating in-memory merge with " + noInMemorySegments + " segments...");

      // When factor is greater or equal to the size of segements, we will ignore
      // intermediate merger, so tmpeDir and tmpFs is useless
      Path tmpDir = null;
      FileSystem tmpFs = null;
      rIter =
          TezMerger.merge(
              conf,
              tmpFs,
              ConfigUtils.getIntermediateInputKeyClass(conf),
              ConfigUtils.getIntermediateInputValueClass(conf),
              inMemorySegments,
              inMemorySegments.size(),
              tmpDir,
              ConfigUtils.getIntermediateInputKeyComparator(conf),
              manager.getProgressable(),
              spilledRecordsCounter,
              null,
              additionalBytesRead,
              null);

      if (null == combiner) {
        TezMerger.writeFile(
            rIter,
            writer,
            manager.getProgressable(),
            TezRuntimeConfiguration.TEZ_RUNTIME_RECORDS_BEFORE_PROGRESS_DEFAULT);
      } else {
        runCombineProcessor(rIter, writer);
      }
      // The compressed length of writer is calculated when called Writer::close, we must
      // update the counter after Writer::close. Counter should be updated in normal
      // execution flow, so do not update counter in finally block.
      writer.close();
      additionalBytesWritten.increment(writer.getCompressedLength());
      writer = null;
      outFileLen = remoteFs.getFileStatus(outputPath).getLen();
      LOG.info(
          inputContext.getUniqueIdentifier()
              + " Merge of the "
              + noInMemorySegments
              + " files in-memory complete."
              + " Remote file is "
              + outputPath
              + " of size "
              + outFileLen);
    } catch (IOException e) {
      remoteFs.delete(outputPath, true);
      throw e;
    } finally {
      if (writer != null) {
        writer.close();
      }
    }

    // Note the output of the merge
    manager.closeOnDiskFile(new FileChunk(outputPath, 0, outFileLen));
  }

  void runCombineProcessor(TezRawKeyValueIterator kvIter, Writer writer)
      throws IOException, InterruptedException {
    combiner.combine(kvIter, writer);
  }

  @Override
  public void cleanup(List<MapOutput> inputs, boolean deleteData)
      throws IOException, InterruptedException {
    if (deleteData) {
      // Additional check at task level
      if (this.manager.isCleanup()) {
        LOG.info("Try deleting stale data");
        MergeManager.cleanup(remoteFs, outputPath);
      }
    }
  }
}
