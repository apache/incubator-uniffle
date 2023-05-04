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

package org.apache.uniffle.server.storage.local;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.uniffle.server.ShuffleDataFlushEvent;
import org.apache.uniffle.storage.common.LocalStorage;
import org.apache.uniffle.storage.util.ShuffleStorageUtils;

public class HashStorageChoosingPolicy implements StorageChoosingPolicy<LocalStorage> {

  @Override
  public LocalStorage choose(ShuffleDataFlushEvent event, LocalStorage... storages) {
    List<LocalStorage> candidates = Arrays.stream(storages)
        .filter(x -> x.canWrite() && !x.isCorrupted())
        .collect(Collectors.toList());

    if (candidates.size() == 0) {
      return null;
    }

    String appId = event.getAppId();
    int shuffleId = event.getShuffleId();
    int partitionId = event.getStartPartition();
    final LocalStorage selectedStorage = candidates.get(
        ShuffleStorageUtils.getStorageIndex(
            candidates.size(),
            appId,
            shuffleId,
            partitionId
        )
    );
    return selectedStorage;
  }
}
