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

package com.tencent.rss.storage.factory;

import java.io.IOException;

import com.tencent.rss.storage.handler.api.ShuffleUploadHandler;
import com.tencent.rss.storage.handler.impl.HdfsShuffleUploadHandler;
import com.tencent.rss.storage.request.CreateShuffleUploadHandlerRequest;
import com.tencent.rss.storage.util.StorageType;

public class ShuffleUploadHandlerFactory {

  public ShuffleUploadHandlerFactory() {

  }

  private static class LazyHolder {
    static final ShuffleUploadHandlerFactory INSTANCE = new ShuffleUploadHandlerFactory();
  }

  public static ShuffleUploadHandlerFactory getInstance() {
    return LazyHolder.INSTANCE;
  }

  public ShuffleUploadHandler createShuffleUploadHandler(
      CreateShuffleUploadHandlerRequest request) throws IOException, RuntimeException {
    if (request.getRemoteStorageType() == StorageType.HDFS) {
      return new HdfsShuffleUploadHandler(
          request.getRemoteStorageBasePath(),
          request.getHadoopConf(),
          request.getHdfsFilePrefix(),
          request.getBufferSize(),
          request.getCombineUpload());
    } else {
      throw new RuntimeException("Unsupported remote storage type " + request.getRemoteStorageType().name());
    }
  }
}
