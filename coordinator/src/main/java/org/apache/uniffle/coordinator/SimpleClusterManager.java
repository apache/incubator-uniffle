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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.impl.grpc.ShuffleServerInternalGrpcClient;
import org.apache.uniffle.client.request.RssCancelDecommissionRequest;
import org.apache.uniffle.client.request.RssDecommissionRequest;
import org.apache.uniffle.common.ServerStatus;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.InvalidRequestException;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.filesystem.HadoopFilesystemProvider;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.coordinator.metric.CoordinatorMetrics;

public class SimpleClusterManager implements ClusterManager {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleClusterManager.class);

  private final Map<String, ServerNode> servers = JavaUtils.newConcurrentMap();
  private final Cache<ServerNode, ShuffleServerInternalGrpcClient> clientCache;
  private Set<String> excludeNodes = Sets.newConcurrentHashSet();
  /** ServerNode whose heartbeat is lost */
  Set<ServerNode> lostNodes = Sets.newHashSet();
  /** Unhealthy ServerNode */
  Set<ServerNode> unhealthyNodes = Sets.newHashSet();
  // tag -> nodes
  private Map<String, Set<ServerNode>> tagToNodes = JavaUtils.newConcurrentMap();
  private AtomicLong excludeLastModify = new AtomicLong(0L);
  private long heartbeatTimeout;
  private volatile int shuffleNodesMax;
  private ScheduledExecutorService scheduledExecutorService;
  private ScheduledExecutorService checkNodesExecutorService;
  private FileSystem hadoopFileSystem;

  private long outputAliveServerCount = 0;
  private final long periodicOutputIntervalTimes;

  private long startTime;
  private boolean startupSilentPeriodEnabled;
  private long startupSilentPeriodDurationMs;
  private boolean readyForServe = false;

  public SimpleClusterManager(CoordinatorConf conf, Configuration hadoopConf) throws Exception {
    this.shuffleNodesMax = conf.getInteger(CoordinatorConf.COORDINATOR_SHUFFLE_NODES_MAX);
    this.heartbeatTimeout = conf.getLong(CoordinatorConf.COORDINATOR_HEARTBEAT_TIMEOUT);
    // the thread for checking if shuffle server report heartbeat in time
    scheduledExecutorService =
        ThreadUtils.getDaemonSingleThreadScheduledExecutor("SimpleClusterManager");

    this.startupSilentPeriodEnabled =
        conf.get(CoordinatorConf.COORDINATOR_START_SILENT_PERIOD_ENABLED);
    this.startupSilentPeriodDurationMs =
        conf.get(CoordinatorConf.COORDINATOR_START_SILENT_PERIOD_DURATION);

    periodicOutputIntervalTimes =
        conf.get(CoordinatorConf.COORDINATOR_NODES_PERIODIC_OUTPUT_INTERVAL_TIMES);

    scheduledExecutorService.scheduleAtFixedRate(
        this::nodesCheck, heartbeatTimeout / 3, heartbeatTimeout / 3, TimeUnit.MILLISECONDS);

    String excludeNodesPath =
        conf.getString(CoordinatorConf.COORDINATOR_EXCLUDE_NODES_FILE_PATH, "");
    if (!StringUtils.isEmpty(excludeNodesPath)) {
      this.hadoopFileSystem =
          HadoopFilesystemProvider.getFilesystem(new Path(excludeNodesPath), hadoopConf);
      long updateNodesInterval =
          conf.getLong(CoordinatorConf.COORDINATOR_EXCLUDE_NODES_CHECK_INTERVAL);
      checkNodesExecutorService =
          ThreadUtils.getDaemonSingleThreadScheduledExecutor("UpdateExcludeNodes");
      checkNodesExecutorService.scheduleAtFixedRate(
          () -> updateExcludeNodes(excludeNodesPath),
          updateNodesInterval,
          updateNodesInterval,
          TimeUnit.MILLISECONDS);
    }

    long clientExpiredTime = conf.get(CoordinatorConf.COORDINATOR_NODES_CLIENT_CACHE_EXPIRED);
    int maxClient = conf.get(CoordinatorConf.COORDINATOR_NODES_CLIENT_CACHE_MAX);
    clientCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(clientExpiredTime, TimeUnit.MILLISECONDS)
            .maximumSize(maxClient)
            .removalListener(
                notify -> ((ShuffleServerInternalGrpcClient) notify.getValue()).close())
            .build();
    this.startTime = System.currentTimeMillis();
  }

  void nodesCheck() {
    try {
      long timestamp = System.currentTimeMillis();
      for (ServerNode sn : servers.values()) {
        if (timestamp - sn.getTimestamp() > heartbeatTimeout) {
          LOG.warn("Heartbeat timeout detect, {} will be removed from node list.", sn);
          sn.setStatus(ServerStatus.LOST);
          lostNodes.add(sn);
          unhealthyNodes.remove(sn);
        } else if (ServerStatus.UNHEALTHY.equals(sn.getStatus())) {
          LOG.warn("Found server {} was unhealthy, will not assign it.", sn);
          unhealthyNodes.add(sn);
          lostNodes.remove(sn);
        } else {
          sn.setStatus(ServerStatus.ACTIVE);
          lostNodes.remove(sn);
          unhealthyNodes.remove(sn);
        }
      }
      for (ServerNode server : lostNodes) {
        ServerNode sn = servers.remove(server.getId());
        if (sn != null) {
          clientCache.invalidate(sn);
          for (Set<ServerNode> nodesWithTag : tagToNodes.values()) {
            nodesWithTag.remove(sn);
          }
        }
      }
      if (!lostNodes.isEmpty() || outputAliveServerCount % periodicOutputIntervalTimes == 0) {
        LOG.info(
            "Alive servers number: {}, ids: {}",
            servers.size(),
            servers.keySet().stream().collect(Collectors.toList()));
      }
      outputAliveServerCount++;

      CoordinatorMetrics.gaugeUnhealthyServerNum.set(unhealthyNodes.size());
      CoordinatorMetrics.gaugeTotalServerNum.set(servers.size());
    } catch (Exception e) {
      LOG.warn("Error happened in nodesCheck", e);
    }
  }

  @VisibleForTesting
  public void nodesCheckTest() {
    nodesCheck();
  }

  private void updateExcludeNodes(String path) {
    int originalExcludeNodesNumber = excludeNodes.size();
    try {
      Path hadoopPath = new Path(path);
      FileStatus fileStatus = hadoopFileSystem.getFileStatus(hadoopPath);
      if (fileStatus != null && fileStatus.isFile()) {
        long latestModificationTime = fileStatus.getModificationTime();
        if (excludeLastModify.get() != latestModificationTime) {
          parseExcludeNodesFile(hadoopFileSystem.open(hadoopPath));
          excludeLastModify.set(latestModificationTime);
        }
      } else {
        excludeNodes = Sets.newConcurrentHashSet();
      }
    } catch (FileNotFoundException fileNotFoundException) {
      excludeNodes = Sets.newConcurrentHashSet();
    } catch (Exception e) {
      LOG.warn("Error when updating exclude nodes, the exclude nodes file path: {}.", path, e);
    }
    int newlyExcludeNodesNumber = excludeNodes.size();
    if (newlyExcludeNodesNumber != originalExcludeNodesNumber) {
      LOG.info("Exclude nodes number: {}, nodes list: {}", newlyExcludeNodesNumber, excludeNodes);
    }
    CoordinatorMetrics.gaugeExcludeServerNum.set(excludeNodes.size());
  }

  private void parseExcludeNodesFile(DataInputStream fsDataInputStream) throws IOException {
    Set<String> nodes = Sets.newConcurrentHashSet();
    try (BufferedReader br =
        new BufferedReader(new InputStreamReader(fsDataInputStream, StandardCharsets.UTF_8))) {
      String line;
      while ((line = br.readLine()) != null) {
        if (!StringUtils.isEmpty(line) && !line.trim().startsWith("#")) {
          nodes.add(line.trim());
        }
      }
    }
    // update exclude nodes and last modify time
    excludeNodes = nodes;
    LOG.info(
        "Updated exclude nodes and {} nodes were marked as exclude nodes", excludeNodes.size());
  }

  @Override
  public void add(ServerNode node) {
    if (!servers.containsKey(node.getId())) {
      LOG.info("Newly registering node: {}", node.getId());
    }
    servers.put(node.getId(), node);
    Set<String> tags = node.getTags();
    // remove node with all tags to deal with the situation of tag change
    for (Set<ServerNode> nodes : tagToNodes.values()) {
      nodes.remove(node);
    }
    // add node to related tags
    for (String tag : tags) {
      tagToNodes.computeIfAbsent(tag, key -> Sets.newConcurrentHashSet());
      tagToNodes.get(tag).add(node);
    }
  }

  @Override
  public List<ServerNode> getServerList(Set<String> requiredTags) {
    List<ServerNode> availableNodes = Lists.newArrayList();
    for (ServerNode node : servers.values()) {
      if (!ServerStatus.ACTIVE.equals(node.getStatus())) {
        continue;
      }
      if (!excludeNodes.contains(node.getId())
          && node.getTags().containsAll(requiredTags)
          && ServerStatus.ACTIVE.equals(node.getStatus())) {
        availableNodes.add(node);
      }
    }
    return availableNodes;
  }

  @Override
  public List<ServerNode> getLostServerList() {
    return Lists.newArrayList(lostNodes);
  }

  @Override
  public List<ServerNode> getUnhealthyServerList() {
    return Lists.newArrayList(unhealthyNodes);
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
  public void clear() {
    servers.clear();
  }

  @Override
  public int getShuffleNodesMax() {
    return shuffleNodesMax;
  }

  @Override
  public boolean isReadyForServe() {
    if (!startupSilentPeriodEnabled) {
      return true;
    }

    if (!readyForServe && System.currentTimeMillis() - startTime > startupSilentPeriodDurationMs) {
      readyForServe = true;
    }

    return readyForServe;
  }

  @Override
  public void decommission(String serverId) {
    ServerNode serverNode = getServerNodeById(serverId);
    getShuffleServerClient(serverNode).decommission(new RssDecommissionRequest());
  }

  @Override
  public void cancelDecommission(String serverId) {
    ServerNode serverNode = getServerNodeById(serverId);
    getShuffleServerClient(serverNode).cancelDecommission(new RssCancelDecommissionRequest());
  }

  private ShuffleServerInternalGrpcClient getShuffleServerClient(ServerNode serverNode) {
    try {
      return clientCache.get(
          serverNode,
          () -> new ShuffleServerInternalGrpcClient(serverNode.getIp(), serverNode.getGrpcPort()));
    } catch (ExecutionException e) {
      throw new RssException(e);
    }
  }

  @Override
  public ServerNode getServerNodeById(String serverId) {
    ServerNode serverNode = servers.get(serverId);
    if (serverNode == null) {
      throw new InvalidRequestException("Server Id [" + serverId + "] not found!");
    }
    return serverNode;
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

  @VisibleForTesting
  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  @VisibleForTesting
  public void setReadyForServe(boolean readyForServe) {
    this.readyForServe = readyForServe;
  }

  @VisibleForTesting
  public void setStartupSilentPeriodEnabled(boolean startupSilentPeriodEnabled) {
    this.startupSilentPeriodEnabled = startupSilentPeriodEnabled;
  }

  @VisibleForTesting
  public Map<String, ServerNode> getServers() {
    return servers;
  }

  @Override
  public void reconfigure(RssConf conf) {
    int nodeMax = conf.getInteger(CoordinatorConf.COORDINATOR_SHUFFLE_NODES_MAX);
    if (nodeMax != shuffleNodesMax) {
      LOG.warn("Coordinator update new shuffleNodesMax {}", nodeMax);
      shuffleNodesMax = nodeMax;
    }
  }

  @Override
  public boolean isPropertyReconfigurable(String property) {
    return CoordinatorConf.COORDINATOR_SHUFFLE_NODES_MAX.key().equals(property);
  }
}
