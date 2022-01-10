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

import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.storage.util.StorageType;

public class StorageManagerFactory {

  private static class LazyHolder {
    static final StorageManagerFactory INSTANCE = new StorageManagerFactory();
  }

  public static StorageManagerFactory getInstance() {
    return LazyHolder.INSTANCE;
  }

  public StorageManager createStorageManager(String serverId, ShuffleServerConf conf) {
    StorageType type = StorageType.valueOf(conf.get(ShuffleServerConf.RSS_STORAGE_TYPE));
    if (StorageType.LOCALFILE.equals(type) || StorageType.MEMORY_LOCALFILE.equals(type)) {
      return new LocalStorageManager(conf);
    } else if (StorageType.HDFS.equals(type) || StorageType.MEMORY_HDFS.equals(type)) {
      return new HdfsStorageManager(conf);
    } else if (StorageType.LOCALFILE_HDFS.equals(type)
        || StorageType.LOCALFILE_HDFS_2.equals(type)
        || StorageType.MEMORY_LOCALFILE_HDFS.equals(type)) {
      return new MultiStorageManager(conf, serverId);
    } else {
      throw new IllegalArgumentException("unknown storageType was found");
    }
  }
}
