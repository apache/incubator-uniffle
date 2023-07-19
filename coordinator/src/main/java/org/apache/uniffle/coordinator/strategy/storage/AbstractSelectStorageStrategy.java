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

package org.apache.uniffle.coordinator.strategy.storage;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.filesystem.HadoopFilesystemProvider;
import org.apache.uniffle.coordinator.ApplicationManager;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.util.CoordinatorUtils;

/**
 * This is a simple implementation class, which provides some methods to check whether the path is
 * normal
 */
public abstract class AbstractSelectStorageStrategy implements SelectStorageStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSelectStorageStrategy.class);
  /** store remote path -> application count for assignment strategy */
  protected final Map<String, RankValue> remoteStoragePathRankValue;

  protected final int fileSize;
  private final String coordinatorId;
  private final Configuration hadoopConf;
  private final CoordinatorConf conf;
  protected List<Map.Entry<String, RankValue>> uris;

  public AbstractSelectStorageStrategy(
      Map<String, RankValue> remoteStoragePathRankValue, CoordinatorConf conf) {
    this.remoteStoragePathRankValue = remoteStoragePathRankValue;
    this.hadoopConf = new Configuration();
    this.fileSize = conf.getInteger(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_SCHEDULE_FILE_SIZE);
    this.coordinatorId =
        conf.getString(CoordinatorUtils.COORDINATOR_ID, UUID.randomUUID().toString());
    this.conf = conf;
  }

  public void readAndWriteHadoopStorage(
      FileSystem fs, Path testPath, String uri, RankValue rankValue) throws IOException {
    byte[] data = RandomUtils.nextBytes(fileSize);
    try (FSDataOutputStream fos = fs.create(testPath)) {
      fos.write(data);
      fos.flush();
    }
    byte[] readData = new byte[fileSize];
    int readBytes;
    try (FSDataInputStream fis = fs.open(testPath)) {
      int hasReadBytes = 0;
      do {
        readBytes = fis.read(readData);
        if (hasReadBytes < fileSize) {
          for (int i = 0; i < readBytes; i++) {
            if (data[hasReadBytes + i] != readData[i]) {
              remoteStoragePathRankValue.put(
                  uri, new RankValue(Long.MAX_VALUE, rankValue.getAppNum().get()));
              throw new RssException("The content of reading and writing is inconsistent.");
            }
          }
        }
        hasReadBytes += readBytes;
      } while (readBytes != -1);
    }
  }

  @Override
  public void detectStorage() {
    uris = Lists.newCopyOnWriteArrayList(remoteStoragePathRankValue.entrySet());
    if (remoteStoragePathRankValue.size() > 1) {
      CountDownLatch countDownLatch = new CountDownLatch(uris.size());
      uris.parallelStream()
          .forEach(
              uri -> {
                if (uri.getKey().startsWith(ApplicationManager.getPathSchema().get(0))) {
                  Path remotePath = new Path(uri.getKey());
                  String rssTest =
                      uri.getKey()
                          + "/rssTest-"
                          + getCoordinatorId()
                          + Thread.currentThread().getName();
                  Path testPath = new Path(rssTest);
                  RankValue rankValue = remoteStoragePathRankValue.get(uri.getKey());
                  rankValue.setHealthy(new AtomicBoolean(true));
                  long startWriteTime = System.currentTimeMillis();
                  try {
                    FileSystem fs = HadoopFilesystemProvider.getFilesystem(remotePath, hadoopConf);
                    for (int j = 0; j < readAndWriteTimes(conf); j++) {
                      readAndWriteHadoopStorage(fs, testPath, uri.getKey(), rankValue);
                    }
                  } catch (Exception e) {
                    LOG.error(
                        "Storage read and write error, we will not use this remote path {}.",
                        uri,
                        e);
                    rankValue.setHealthy(new AtomicBoolean(false));
                  } finally {
                    sortPathByRankValue(uri.getKey(), rssTest, startWriteTime);
                  }
                  countDownLatch.countDown();
                }
              });
      try {
        countDownLatch.await();
      } catch (InterruptedException e) {
        LOG.error("Failed to detectStorage!");
      }
    }
  }

  @VisibleForTesting
  public void sortPathByRankValue(String path, String testPath, long startWrite) {
    RankValue rankValue = remoteStoragePathRankValue.get(path);
    try {
      FileSystem fs = HadoopFilesystemProvider.getFilesystem(new Path(path), hadoopConf);
      fs.delete(new Path(testPath), true);
      if (rankValue.getHealthy().get()) {
        rankValue.setCostTime(new AtomicLong(System.currentTimeMillis() - startWrite));
      }
    } catch (Exception e) {
      rankValue.setCostTime(new AtomicLong(Long.MAX_VALUE));
      LOG.error("Failed to sort, we will not use this remote path {}.", path, e);
    }

    if (this.getComparator() != null) {
      uris =
          Lists.newCopyOnWriteArrayList(remoteStoragePathRankValue.entrySet()).stream()
              .filter(Objects::nonNull)
              .sorted(this.getComparator())
              .collect(Collectors.toList());
    } else {
      uris =
          Lists.newCopyOnWriteArrayList(remoteStoragePathRankValue.entrySet()).stream()
              .filter(Objects::nonNull)
              .collect(Collectors.toList());
    }
    LOG.info("The sorted remote path list is: {}", uris);
  }

  protected int readAndWriteTimes(CoordinatorConf conf) {
    return 1;
  }

  /**
   * Different strategies will have different sorting methods of remote paths
   *
   * @return A comparator is to calculate the RankValue
   */
  protected Comparator<Map.Entry<String, RankValue>> getComparator() {
    return null;
  }

  String getCoordinatorId() {
    return coordinatorId;
  }
}
