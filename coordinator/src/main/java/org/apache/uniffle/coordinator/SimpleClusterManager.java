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

package org.apache.uniffle.coordinator;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.util.ThreadUtils;

public class SimpleClusterManager implements ClusterManager {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleClusterManager.class);

  private final Map<String, ServerNode> servers = Maps.newConcurrentMap();
  private Set<String> excludeNodes = Sets.newConcurrentHashSet();
  // tag -> nodes
  private Map<String, Set<ServerNode>> tagToNodes = Maps.newConcurrentMap();
  private AtomicLong excludeLastModify = new AtomicLong(0L);
  private long heartbeatTimeout;
  private int shuffleNodesMax;
  private ScheduledExecutorService scheduledExecutorService;
  private ScheduledExecutorService checkNodesExecutorService;
  private FileSystem hadoopFileSystem;

  public SimpleClusterManager(CoordinatorConf conf, Configuration hadoopConf) throws IOException {
    this.shuffleNodesMax = conf.getInteger(CoordinatorConf.COORDINATOR_SHUFFLE_NODES_MAX);
    this.heartbeatTimeout = conf.getLong(CoordinatorConf.COORDINATOR_HEARTBEAT_TIMEOUT);
    // the thread for checking if shuffle server report heartbeat in time
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        ThreadUtils.getThreadFactory("SimpleClusterManager-%d"));
    scheduledExecutorService.scheduleAtFixedRate(
        () -> nodesCheck(), heartbeatTimeout / 3,
        heartbeatTimeout / 3, TimeUnit.MILLISECONDS);

    String excludeNodesPath = conf.getString(CoordinatorConf.COORDINATOR_EXCLUDE_NODES_FILE_PATH, "");
    if (!StringUtils.isEmpty(excludeNodesPath)) {
      this.hadoopFileSystem = CoordinatorUtils.getFileSystemForPath(new Path(excludeNodesPath), hadoopConf);
      long updateNodesInterval = conf.getLong(CoordinatorConf.COORDINATOR_EXCLUDE_NODES_CHECK_INTERVAL);
      checkNodesExecutorService = Executors.newSingleThreadScheduledExecutor(
          ThreadUtils.getThreadFactory("UpdateExcludeNodes-%d"));
      checkNodesExecutorService.scheduleAtFixedRate(
          () -> updateExcludeNodes(excludeNodesPath), updateNodesInterval, updateNodesInterval, TimeUnit.MILLISECONDS);
    }
  }

  void nodesCheck() {
    try {
      long timestamp = System.currentTimeMillis();
      Set<String> deleteIds = Sets.newHashSet();
      for (ServerNode sn : servers.values()) {
        if (timestamp - sn.getTimestamp() > heartbeatTimeout) {
          LOG.warn("Heartbeat timeout detect, " + sn + " will be removed from node list.");
          deleteIds.add(sn.getId());
        }
      }
      for (String serverId : deleteIds) {
        ServerNode sn = servers.remove(serverId);
        if (sn != null) {
          for (Set<ServerNode> nodesWithTag : tagToNodes.values()) {
            nodesWithTag.remove(sn);
          }
        }
      }

      CoordinatorMetrics.gaugeTotalServerNum.set(servers.size());
    } catch (Exception e) {
      LOG.warn("Error happened in nodesCheck", e);
    }
  }

  private void updateExcludeNodes(String path) {
    try {
      Path hadoopPath = new Path(path);
      FileStatus fileStatus = hadoopFileSystem.getFileStatus(hadoopPath);
      if (fileStatus != null && fileStatus.isFile()) {
        if (excludeLastModify.get() != fileStatus.getModificationTime()) {
          parseExcludeNodesFile(hadoopFileSystem.open(hadoopPath));
          excludeLastModify.set(fileStatus.getModificationTime());
        }
      } else {
        excludeNodes = Sets.newConcurrentHashSet();
      }
      CoordinatorMetrics.gaugeExcludeServerNum.set(excludeNodes.size());
    } catch (FileNotFoundException fileNotFoundException) {
      excludeNodes = Sets.newConcurrentHashSet();
    } catch (Exception e) {
      LOG.warn("Error when updating exclude nodes, the exclude nodes file path: " + path, e);
    }
  }

  private void parseExcludeNodesFile(DataInputStream fsDataInputStream) throws IOException {
    Set<String> nodes = Sets.newConcurrentHashSet();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fsDataInputStream, StandardCharsets.UTF_8))) {
      String line;
      while ((line = br.readLine()) != null) {
        if (!StringUtils.isEmpty(line)) {
          nodes.add(line);
        }
      }
    }
    // update exclude nodes and last modify time
    excludeNodes = nodes;
    LOG.info("Updated exclude nodes and " + excludeNodes.size() + " nodes were marked as exclude nodes");
  }

  @Override
  public void add(ServerNode node) {
    servers.put(node.getId(), node);
    Set<String> tags = node.getTags();
    // remove node with all tags to deal with the situation of tag change
    for (Set<ServerNode> nodes : tagToNodes.values()) {
      nodes.remove(node);
    }
    // add node to related tags
    for (String tag : tags) {
      tagToNodes.putIfAbsent(tag, Sets.newConcurrentHashSet());
      tagToNodes.get(tag).add(node);
    }
  }

  @Override
  public List<ServerNode> getServerList(Set<String> requiredTags) {
    List<ServerNode> availableNodes = Lists.newArrayList();
    for (ServerNode node : servers.values()) {
      if (node.isDecommissioned()) {
        continue;
      }
      if (!excludeNodes.contains(node.getId())
          && node.getTags().containsAll(requiredTags)
          && node.isHealthy()) {
        availableNodes.add(node);
      }
    }
    return availableNodes;
  }

  public Set<String> getExcludeNodes() {
    return excludeNodes;
  }

  public Map<String, Set<ServerNode>> getTagToNodes() {
    return tagToNodes;
  }

  @Override
  public int getNodesNum() {
    return servers.size();
  }

  @Override
  public List<ServerNode> list() {
    return Lists.newArrayList(servers.values());
  }

  @VisibleForTesting
  void clear() {
    servers.clear();
  }

  @Override
  public int getShuffleNodesMax() {
    return shuffleNodesMax;
  }

  @Override
  public void close() throws IOException {
    if (hadoopFileSystem != null) {
      hadoopFileSystem.close();
    }

    if (scheduledExecutorService != null) {
      scheduledExecutorService.shutdown();
    }

    if (checkNodesExecutorService != null) {
      checkNodesExecutorService.shutdown();
    }
  }
}
