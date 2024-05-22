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

import java.io.IOException;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.tez.common.RssTezConfig;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.DiskFetchedInput;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput.Type;
import org.apache.tez.runtime.library.common.shuffle.MemoryFetchedInput;
import org.apache.tez.runtime.library.common.shuffle.RemoteFetchedInput;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutputFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.filesystem.HadoopFilesystemProvider;

import static org.apache.tez.common.RssTezConfig.RSS_REMOTE_SPILL_STORAGE_PATH;

/** Usage: Create instance, setInitialMemoryAvailable(long), configureAndStart() */
@Private
public class RssSimpleFetchedInputAllocator extends SimpleFetchedInputAllocator {

  private static final Logger LOG = LoggerFactory.getLogger(RssSimpleFetchedInputAllocator.class);
  // In order to be compatible with the Tez IFile file format, the decoded data needs to be added
  // with the corresponding HEADER and CHECKSUM, which occupy 8 bytes.
  private static final int IFILE_HEAD_CHECKSUM_LEN = 8;

  private final Configuration conf;

  private final TezTaskOutputFiles fileNameAllocator;
  private final LocalDirAllocator localDirAllocator;

  // Configuration parameters
  private final long memoryLimit;
  private final long maxSingleShuffleLimit;

  private final long maxAvailableTaskMemory;
  private final long initialMemoryAvailable;

  private final String srcNameTrimmed;

  private volatile long usedMemory = 0;

  private final String uniqueIdentifier;
  private final String appAttemptId;
  private final boolean remoteSpillEnable;
  private FileSystem remoteFS;
  private String remoteSpillBasePath;

  public RssSimpleFetchedInputAllocator(
      String srcNameTrimmed,
      String uniqueIdentifier,
      int dagID,
      Configuration conf,
      long maxTaskAvailableMemory,
      long memoryAvailable,
      String appAttemptId) {
    super(srcNameTrimmed, uniqueIdentifier, dagID, conf, maxTaskAvailableMemory, memoryAvailable);
    this.srcNameTrimmed = srcNameTrimmed;
    this.conf = conf;
    this.maxAvailableTaskMemory = maxTaskAvailableMemory;
    this.initialMemoryAvailable = memoryAvailable;
    this.uniqueIdentifier = uniqueIdentifier;
    this.appAttemptId = appAttemptId;

    this.fileNameAllocator = new TezTaskOutputFiles(conf, uniqueIdentifier, dagID);
    this.localDirAllocator = new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);

    // Setup configuration
    final float maxInMemCopyUse =
        conf.getFloat(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT_DEFAULT);
    if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
      throw new RssException(
          "Invalid value for "
              + TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT
              + ": "
              + maxInMemCopyUse);
    }

    long memReq =
        (long)
            (conf.getLong(
                    Constants.TEZ_RUNTIME_TASK_MEMORY,
                    Math.min(maxAvailableTaskMemory, Integer.MAX_VALUE))
                * maxInMemCopyUse);

    if (memReq <= this.initialMemoryAvailable) {
      this.memoryLimit = memReq;
    } else {
      this.memoryLimit = initialMemoryAvailable;
    }

    final float singleShuffleMemoryLimitPercent =
        conf.getFloat(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT_DEFAULT);
    if (singleShuffleMemoryLimitPercent <= 0.0f || singleShuffleMemoryLimitPercent > 1.0f) {
      throw new RssException(
          "Invalid value for "
              + TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT
              + ": "
              + singleShuffleMemoryLimitPercent);
    }
    this.maxSingleShuffleLimit =
        (long) Math.min((memoryLimit * singleShuffleMemoryLimitPercent), Integer.MAX_VALUE);
    this.remoteSpillEnable = conf.getBoolean(RssTezConfig.RSS_REDUCE_REMOTE_SPILL_ENABLED, false);
    if (this.remoteSpillEnable) {
      this.remoteSpillBasePath = conf.get(RSS_REMOTE_SPILL_STORAGE_PATH);
      if (StringUtils.isBlank(this.remoteSpillBasePath)) {
        throw new RssException("You must set remote spill path!");
      }
      // construct remote configuration
      String remoteStorageConf = this.conf.get(RssTezConfig.RSS_REMOTE_STORAGE_CONF);
      Map<String, String> remoteStorageConfMap =
          RemoteStorageInfo.parseRemoteStorageConf(remoteStorageConf);
      Configuration remoteConf = new Configuration(this.conf);
      for (Map.Entry<String, String> entry : remoteStorageConfMap.entrySet()) {
        remoteConf.set(entry.getKey(), entry.getValue());
      }
      // construct remote filesystem
      int replication =
          this.conf.getInt(
              RssTezConfig.RSS_REDUCE_REMOTE_SPILL_REPLICATION,
              RssTezConfig.RSS_REDUCE_REMOTE_SPILL_REPLICATION_DEFAULT);
      int retries =
          this.conf.getInt(
              RssTezConfig.RSS_REDUCE_REMOTE_SPILL_RETRIES,
              RssTezConfig.RSS_REDUCE_REMOTE_SPILL_RETRIES_DEFAULT);
      try {
        remoteConf.setInt("dfs.replication", replication);
        remoteConf.setInt("dfs.client.block.write.retries", retries);
        remoteFS =
            HadoopFilesystemProvider.getFilesystem(new Path(this.remoteSpillBasePath), remoteConf);
      } catch (Exception e) {
        throw new RssException("Cannot init remoteFS on path:" + this.remoteSpillBasePath);
      }
    }

    LOG.info(
        srcNameTrimmed
            + ": "
            + "RequestedMemory="
            + memReq
            + ", AssignedMemory="
            + this.memoryLimit
            + ", maxSingleShuffleLimit="
            + this.maxSingleShuffleLimit);
  }

  @Private
  public static long getInitialMemoryReq(Configuration conf, long maxAvailableTaskMemory) {
    final float maxInMemCopyUse =
        conf.getFloat(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT_DEFAULT);
    if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
      throw new RssException(
          "Invalid value for "
              + TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT
              + ": "
              + maxInMemCopyUse);
    }
    return (long)
        (conf.getLong(
                Constants.TEZ_RUNTIME_TASK_MEMORY,
                Math.min(maxAvailableTaskMemory, Integer.MAX_VALUE))
            * maxInMemCopyUse);
  }

  @Override
  public synchronized FetchedInput allocate(
      long actualSize, long compressedSize, InputAttemptIdentifier inputAttemptIdentifier)
      throws IOException {
    if (actualSize > maxSingleShuffleLimit || this.usedMemory + actualSize > this.memoryLimit) {
      if (remoteSpillEnable) {
        LOG.info("Allocate RemoteFetchedInput, length:{}", actualSize);
        return new RemoteFetchedInput(
            actualSize + IFILE_HEAD_CHECKSUM_LEN,
            inputAttemptIdentifier,
            this,
            remoteFS,
            remoteSpillBasePath,
            uniqueIdentifier,
            appAttemptId);
      } else {
        LOG.info("Allocate DiskFetchedInput, length:{}", actualSize);
        return new DiskFetchedInput(
            actualSize + IFILE_HEAD_CHECKSUM_LEN,
            inputAttemptIdentifier,
            this,
            conf,
            localDirAllocator,
            fileNameAllocator);
      }
    } else {
      this.usedMemory += actualSize;
      if (LOG.isDebugEnabled()) {
        LOG.info(
            srcNameTrimmed
                + ": "
                + "Used memory after allocating "
                + actualSize
                + " : "
                + usedMemory);
      }
      return new MemoryFetchedInput(actualSize, inputAttemptIdentifier, this);
    }
  }

  @Override
  public synchronized FetchedInput allocateType(
      Type type,
      long actualSize,
      long compressedSize,
      InputAttemptIdentifier inputAttemptIdentifier)
      throws IOException {

    switch (type) {
      case DISK:
        // It should not be called here.
        if (remoteSpillEnable) {
          LOG.info("AllocateType RemoteFetchedInput, compressedSize:{}", compressedSize);
          return new RemoteFetchedInput(
              actualSize + IFILE_HEAD_CHECKSUM_LEN,
              inputAttemptIdentifier,
              this,
              remoteFS,
              remoteSpillBasePath,
              uniqueIdentifier,
              appAttemptId);
        } else {
          LOG.info("AllocateType DiskFetchedInput, compressedSize:{}", compressedSize);
          return new DiskFetchedInput(
              actualSize + IFILE_HEAD_CHECKSUM_LEN,
              inputAttemptIdentifier,
              this,
              conf,
              localDirAllocator,
              fileNameAllocator);
        }
      default:
        return allocate(actualSize, compressedSize, inputAttemptIdentifier);
    }
  }

  @Override
  public synchronized void fetchComplete(FetchedInput fetchedInput) {
    switch (fetchedInput.getType()) {
        // Not tracking anything here.
      case DISK:
      case DISK_DIRECT:
      case MEMORY:
        break;
      default:
        throw new RssException(
            "InputType: " + fetchedInput.getType() + " not expected for Broadcast fetch");
    }
  }

  @Override
  public synchronized void fetchFailed(FetchedInput fetchedInput) {
    cleanup(fetchedInput);
  }

  @Override
  public synchronized void freeResources(FetchedInput fetchedInput) {
    cleanup(fetchedInput);
  }

  private void cleanup(FetchedInput fetchedInput) {
    switch (fetchedInput.getType()) {
      case DISK:
        break;
      case MEMORY:
        unreserve(((MemoryFetchedInput) fetchedInput).getSize());
        break;
      default:
        throw new RssException(
            "InputType: " + fetchedInput.getType() + " not expected for Broadcast fetch");
    }
  }

  private synchronized void unreserve(long size) {
    this.usedMemory -= size;
    if (LOG.isDebugEnabled()) {
      LOG.debug("{}: Used memory after freeing {}: {}", srcNameTrimmed, size, usedMemory);
    }
  }

  @VisibleForTesting
  public void setRemoteFS(FileSystem remoteFS) {
    this.remoteFS = remoteFS;
  }
}
