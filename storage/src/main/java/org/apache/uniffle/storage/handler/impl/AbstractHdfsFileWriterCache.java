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

package org.apache.uniffle.storage.handler.impl;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractHdfsFileWriterCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractHdfsFileWriterCache.class);

  private static Cache<String, HdfsFileWriter> fileWriterCache;

  AbstractHdfsFileWriterCache(long expireSec) {
    if (fileWriterCache == null) {
      synchronized (AbstractHdfsFileWriterCache.class) {
        if (fileWriterCache == null) {
          fileWriterCache = CacheBuilder
              .newBuilder()
              .expireAfterAccess(expireSec, TimeUnit.SECONDS)
              .removalListener((RemovalListener<String, HdfsFileWriter>) notification -> {
                try {
                  notification.getValue().close();
                } catch (IOException e) {
                  LOGGER.error("Errors on closing hdfs file writer, path: {}",
                      notification.getValue().getPath(), e);
                }
              })
              .build();
        }
      }
    }
  }

  public HdfsFileWriter getFromCache(String key) {
    return fileWriterCache.getIfPresent(key);
  }

  public void putToCache(String key, HdfsFileWriter writer) {
    fileWriterCache.put(key, writer);
  }

  public void deleteByKey(String key) {
    fileWriterCache.invalidate(key);
  }
}

