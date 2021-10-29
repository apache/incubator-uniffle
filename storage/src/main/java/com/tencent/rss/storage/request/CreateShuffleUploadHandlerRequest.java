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

package com.tencent.rss.storage.request;

import com.tencent.rss.storage.util.StorageType;
import org.apache.hadoop.conf.Configuration;

/**
 * CreateShuffleUploadHandlerRequest is used to hold the parameters to create remote storage for shuffle uploader.
 * For now only HDFS is supported, but COS, OZONE and other storage system will be supported in the future.
 */
public class CreateShuffleUploadHandlerRequest {
  private final StorageType remoteStorageType;
  private final String shuffleKey;
  private final String remoteStorageBasePath;
  private final String hdfsFilePrefix;
  private final Configuration hadoopConf;
  private final int bufferSize;
  private final boolean combineUpload;

  public StorageType getRemoteStorageType() {
    return remoteStorageType;
  }

  public String getShuffleKey() {
    return shuffleKey;
  }

  public String getRemoteStorageBasePath() {
    return remoteStorageBasePath;
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  public String getHdfsFilePrefix() {
    return hdfsFilePrefix;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public boolean getCombineUpload() {
    return combineUpload;
  }

  public static class Builder {
    private StorageType remoteStorageType;
    private String shuffleKey;
    private String remoteStorageBasePath;
    private String hdfsFilePrefix;
    private Configuration hadoopConf;
    private int bufferSize;
    private boolean combineUpload;

    public Builder() {
      // use HDFS by default, we may use COS, OZONE in the future
      this.remoteStorageType = StorageType.HDFS;
      this.bufferSize = 4096;
      this.combineUpload = true;
    }

    public Builder remoteStorageType(StorageType remoteStorageType) {
      this.remoteStorageType = remoteStorageType;
      return this;
    }

    public Builder shuffleKey(String shuffleKey) {
      this.shuffleKey = shuffleKey;
      return this;
    }

    public Builder remoteStorageBasePath(String remoteStorageBasePath) {
      this.remoteStorageBasePath = remoteStorageBasePath;
      return this;
    }

    public Builder hdfsFilePrefix(String hdfsFilePrefix) {
      this.hdfsFilePrefix = hdfsFilePrefix;
      return this;
    }

    public Builder hadoopConf(Configuration hadoopConf) {
      this.hadoopConf = hadoopConf;
      return this;
    }

    public Builder bufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    public Builder combineUpload(boolean combineUpload) {
      this.combineUpload = combineUpload;
      return this;
    }

    public CreateShuffleUploadHandlerRequest build() throws IllegalArgumentException {
      validate();
      return new CreateShuffleUploadHandlerRequest(this);
    }

    private void validate() throws IllegalArgumentException {
      if (remoteStorageType == null) {
        throw new IllegalArgumentException("Remote storage type must be set");
      }

      // We only support HDFS at present, so only check HDFS related parameters
      if (remoteStorageType == StorageType.HDFS) {
        if (remoteStorageBasePath == null || remoteStorageBasePath.isEmpty()) {
          throw new IllegalArgumentException("Remote storage path must be set");
        }

        if (hdfsFilePrefix == null || hdfsFilePrefix.isEmpty()) {
          throw new IllegalArgumentException("File prefix must be set");
        }

        if (hadoopConf == null) {
          throw new IllegalArgumentException("Hadoop conf must be set");
        }

        if (bufferSize <= 1024) {
          throw new IllegalArgumentException("Buffer size must be larger than 1K");
        }
      }
    }

  }

  private CreateShuffleUploadHandlerRequest(Builder builder) {
    this.remoteStorageType = builder.remoteStorageType;
    this.shuffleKey = builder.shuffleKey;
    this.remoteStorageBasePath = builder.remoteStorageBasePath;
    this.hdfsFilePrefix = builder.hdfsFilePrefix;
    this.hadoopConf = builder.hadoopConf;
    this.bufferSize = builder.bufferSize;
    this.combineUpload = builder.combineUpload;
  }
}
