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

package com.tencent.rss.storage.common;

import java.io.IOException;

import com.tencent.rss.storage.handler.api.ServerReadHandler;
import com.tencent.rss.storage.handler.api.ShuffleWriteHandler;
import com.tencent.rss.storage.request.CreateShuffleReadHandlerRequest;
import com.tencent.rss.storage.request.CreateShuffleWriteHandlerRequest;


public interface Storage {

  boolean canWrite();

  boolean lockShuffleShared(String shuffleKey);

  boolean unlockShuffleShared(String shuffleKey);

  boolean lockShuffleExcluded(String shuffleKey);

  boolean unlockShuffleExcluded(String shuffleKey);

  void updateWriteMetrics(StorageWriteMetrics metrics);

  void updateReadMetrics(StorageReadMetrics metrics);

  ShuffleWriteHandler getOrCreateWriteHandler(CreateShuffleWriteHandlerRequest request) throws IOException;

  ServerReadHandler getOrCreateReadHandler(CreateShuffleReadHandlerRequest request);

  CreateShuffleWriteHandlerRequest getCreateWriterHandlerRequest(String appId, int shuffleId, int partition);

  void removeHandlers(String appId);

  void createMetadataIfNotExist(String shuffleKey);

  String getStoragePath();
}
