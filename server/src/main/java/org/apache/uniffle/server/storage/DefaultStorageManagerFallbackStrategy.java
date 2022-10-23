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

package org.apache.uniffle.server.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.server.ShuffleDataFlushEvent;
import org.apache.uniffle.server.ShuffleServerConf;

public class DefaultStorageManagerFallbackStrategy extends AbstractStorageManagerFallbackStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultStorageManagerFallbackStrategy.class);
  private final Long fallBackTimes;

  public DefaultStorageManagerFallbackStrategy(ShuffleServerConf conf) {
    super(conf);
    fallBackTimes = conf.get(ShuffleServerConf.FALLBACK_MAX_FAIL_TIMES);
  }

  @Override
  public StorageManager tryFallback(StorageManager current, ShuffleDataFlushEvent event, StorageManager... options) {
    if (fallBackTimes > 0
        && (event.getRetryTimes() < fallBackTimes || event.getRetryTimes() % fallBackTimes > 0)) {
      return current;
    }
    int nextIdx = -1;
    for (int i = 0; i < options.length; i++) {
      if (current == options[i]) {
        nextIdx = (i + 1) % options.length;
        break;
      }
    }
    if (nextIdx == -1) {
      throw new RuntimeException("Current StorageManager is not in options");
    }
    for (int i = 0; i < options.length - 1; i++) {
      StorageManager storageManager = options[(i + nextIdx) % options.length];
      if (!storageManager.canWrite(event)) {
        continue;
      }
      return storageManager;
    }
    LOG.warn("Fallback failed, all storageManagers can not write.");
    return current;
  }
}
