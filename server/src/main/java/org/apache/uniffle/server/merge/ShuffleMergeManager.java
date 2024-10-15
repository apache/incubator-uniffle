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

package org.apache.uniffle.server.merge;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.merger.Segment;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;

import static org.apache.uniffle.server.ShuffleServerConf.SERVER_MERGE_CLASS_LOADER_JARS_PATH;

public class ShuffleMergeManager {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleMergeManager.class);
  public static final String MERGE_APP_SUFFIX = "@RemoteMerge";

  private ShuffleServerConf serverConf;
  private final ShuffleServer shuffleServer;
  // appId -> shuffleid -> Shuffle
  private final Map<String, Map<Integer, Shuffle>> shuffles = JavaUtils.newConcurrentMap();
  private final MergeEventHandler eventHandler;
  private final Map<String, ClassLoader> cachedClassLoader = new HashMap<>();

  // If comparator is not set, will use hashCode to compare. It is used for shuffle that does not
  // require
  // sort but require combine.
  private Comparator defaultComparator =
      new Comparator() {
        @Override
        public int compare(Object o1, Object o2) {
          int h1 = (o1 == null) ? 0 : o1.hashCode();
          int h2 = (o2 == null) ? 0 : o2.hashCode();
          return h1 < h2 ? -1 : h1 == h2 ? 0 : 1;
        }
      };

  public ShuffleMergeManager(ShuffleServerConf serverConf, ShuffleServer shuffleServer)
      throws Exception {
    this.serverConf = serverConf;
    this.shuffleServer = shuffleServer;
    this.eventHandler = new DefaultMergeEventHandler(this.serverConf, this::processEvent);
    initCacheClassLoader();
  }

  public void initCacheClassLoader() throws Exception {
    addCacheClassLoader("", serverConf.getString(SERVER_MERGE_CLASS_LOADER_JARS_PATH));
    Map<String, Object> props =
        serverConf.getPropsWithPrefix(SERVER_MERGE_CLASS_LOADER_JARS_PATH.key() + ".");
    for (Map.Entry<String, Object> prop : props.entrySet()) {
      addCacheClassLoader(prop.getKey(), (String) prop.getValue());
    }
  }

  public void addCacheClassLoader(String label, String jarsPath) throws Exception {
    if (StringUtils.isNotBlank(jarsPath)) {
      File jarsPathFile = new File(jarsPath);
      if (jarsPathFile.exists()) {
        if (jarsPathFile.isFile()) {
          URLClassLoader urlClassLoader =
              AccessController.doPrivileged(
                  new PrivilegedExceptionAction<URLClassLoader>() {
                    @Override
                    public URLClassLoader run() throws Exception {
                      return new URLClassLoader(
                          new URL[] {new URL("file://" + jarsPath)},
                          Thread.currentThread().getContextClassLoader());
                    }
                  });
          cachedClassLoader.put(label, urlClassLoader);
        } else if (jarsPathFile.isDirectory()) {
          File[] files = jarsPathFile.listFiles();
          List<URL> urlList = new ArrayList<>();
          if (files != null) {
            for (File file : files) {
              if (file.getName().endsWith(".jar")) {
                urlList.add(new URL("file://" + file.getAbsolutePath()));
              }
            }
          }
          URLClassLoader urlClassLoader =
              AccessController.doPrivileged(
                  new PrivilegedExceptionAction<URLClassLoader>() {
                    @Override
                    public URLClassLoader run() throws Exception {
                      return new URLClassLoader(
                          urlList.toArray(new URL[urlList.size()]),
                          Thread.currentThread().getContextClassLoader());
                    }
                  });
          cachedClassLoader.put(label, urlClassLoader);
        } else {
          // If not set, will use current thread classloader
          cachedClassLoader.put(label, Thread.currentThread().getContextClassLoader());
        }
      }
    } else {
      // If not set, will use current thread classloader
      cachedClassLoader.put(label, Thread.currentThread().getContextClassLoader());
    }
  }

  public ClassLoader getClassLoader(String label) {
    if (StringUtils.isBlank(label)) {
      return cachedClassLoader.get("");
    }
    return cachedClassLoader.getOrDefault(label, cachedClassLoader.get(""));
  }

  public StatusCode registerShuffle(
      String appId,
      int shuffleId,
      String keyClassName,
      String valueClassName,
      String comparatorClassName,
      int mergedBlockSize,
      String classLoaderLabel) {
    try {
      ClassLoader classLoader = getClassLoader(classLoaderLabel);
      Class kClass = ClassUtils.getClass(classLoader, keyClassName);
      Class vClass = ClassUtils.getClass(classLoader, valueClassName);
      Comparator comparator;
      if (StringUtils.isNotBlank(comparatorClassName)) {
        Constructor constructor =
            ClassUtils.getClass(classLoader, comparatorClassName).getDeclaredConstructor();
        constructor.setAccessible(true);
        comparator = (Comparator) constructor.newInstance();
      } else {
        comparator = defaultComparator;
      }
      this.shuffles.putIfAbsent(appId, JavaUtils.newConcurrentMap());
      this.shuffles
          .get(appId)
          .putIfAbsent(
              shuffleId,
              new Shuffle(
                  serverConf,
                  eventHandler,
                  shuffleServer,
                  appId,
                  shuffleId,
                  kClass,
                  vClass,
                  comparator,
                  mergedBlockSize,
                  classLoader));
    } catch (ClassNotFoundException
        | InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      LOG.info("Cant register shuffle, caused by ", e);
      removeBuffer(appId, shuffleId);
      return StatusCode.INTERNAL_ERROR;
    }
    return StatusCode.SUCCESS;
  }

  public void removeBuffer(String appId) {
    if (this.shuffles.containsKey(appId)) {
      for (Integer shuffleId : this.shuffles.get(appId).keySet()) {
        removeBuffer(appId, shuffleId);
      }
    }
  }

  public void removeBuffer(String appId, List<Integer> shuffleIds) {
    if (this.shuffles.containsKey(appId)) {
      for (Integer shuffleId : shuffleIds) {
        removeBuffer(appId, shuffleId);
      }
    }
  }

  public void removeBuffer(String appId, int shuffleId) {
    if (this.shuffles.containsKey(appId)) {
      if (this.shuffles.get(appId).containsKey(shuffleId)) {
        this.shuffles.get(appId).get(shuffleId).cleanup();
        this.shuffles.get(appId).remove(shuffleId);
      }
      if (this.shuffles.get(appId).size() == 0) {
        this.shuffles.remove(appId);
      }
    }
  }

  public void startSortMerge(
      String appId, int shuffleId, int partitionId, Roaring64NavigableMap expectedBlockIdMap)
      throws IOException {
    Map<Integer, Shuffle> shuffleMap = this.shuffles.get(appId);
    if (shuffleMap != null) {
      Shuffle shuffle = shuffleMap.get(shuffleId);
      if (shuffle != null) {
        shuffle.startSortMerge(partitionId, expectedBlockIdMap);
      }
    }
  }

  public void processEvent(MergeEvent event) {
    try {
      ClassLoader original = Thread.currentThread().getContextClassLoader();
      Thread.currentThread()
          .setContextClassLoader(
              this.getShuffle(event.getAppId(), event.getShuffleId()).getClassLoader());
      List<Segment> segments =
          this.getPartition(event.getAppId(), event.getShuffleId(), event.getPartitionId())
              .getSegments(
                  serverConf,
                  event.getExpectedBlockIdMap().iterator(),
                  event.getKeyClass(),
                  event.getValueClass());
      this.getPartition(event.getAppId(), event.getShuffleId(), event.getPartitionId())
          .merge(segments);
      Thread.currentThread().setContextClassLoader(original);
    } catch (Exception e) {
      LOG.info("Found exception when merge, caused by ", e);
      throw new RssException(e);
    }
  }

  public ShuffleDataResult getShuffleData(
      String appId, int shuffleId, int partitionId, long blockId) throws IOException {
    return this.getPartition(appId, shuffleId, partitionId).getShuffleData(blockId);
  }

  public MergeStatus tryGetBlock(String appId, int shuffleId, int partitionId, long blockId) {
    return this.getPartition(appId, shuffleId, partitionId).tryGetBlock(blockId);
  }

  @VisibleForTesting
  MergeEventHandler getEventHandler() {
    return eventHandler;
  }

  Shuffle getShuffle(String appId, int shuffleId) {
    return this.shuffles.get(appId).get(shuffleId);
  }

  @VisibleForTesting
  Partition getPartition(String appId, int shuffleId, int partitionId) {
    return this.shuffles.get(appId).get(shuffleId).getPartition(partitionId);
  }

  public void refreshAppId(String appId) {
    shuffleServer.getShuffleTaskManager().refreshAppId(appId + MERGE_APP_SUFFIX);
  }
}
