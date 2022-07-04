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

package org.apache.uniffle.storage.factory;

import java.io.IOException;

import org.apache.uniffle.storage.handler.api.ShuffleUploadHandler;
import org.apache.uniffle.storage.handler.impl.HdfsShuffleUploadHandler;
import org.apache.uniffle.storage.request.CreateShuffleUploadHandlerRequest;
import org.apache.uniffle.storage.util.StorageType;

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
