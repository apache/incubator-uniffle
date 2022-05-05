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

package com.tencent.rss.server.storage;

import java.util.Set;

import com.tencent.rss.server.Checker;
import com.tencent.rss.server.ShuffleDataFlushEvent;
import com.tencent.rss.server.ShuffleDataReadEvent;
import com.tencent.rss.storage.common.Storage;
import com.tencent.rss.storage.handler.api.ShuffleWriteHandler;


public interface StorageManager {

  Storage selectStorage(ShuffleDataFlushEvent event);

  Storage selectStorage(ShuffleDataReadEvent event);

  boolean write(Storage storage, ShuffleWriteHandler handler, ShuffleDataFlushEvent event);

  void updateWriteMetrics(ShuffleDataFlushEvent event, long writeTime);

  // todo: add an interface for updateReadMetrics

  void removeResources(String appId, Set<Integer> shuffleSet);

  void start();

  void stop();

  void registerRemoteStorage(String appId, String remoteStorage);

  Checker getStorageChecker();

  // todo: add an interface that check storage isHealthy
}
