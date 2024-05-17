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

package org.apache.uniffle.flink.shuffle;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.shuffle.JobShuffleContext;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleMasterContext;
import org.apache.flink.runtime.shuffle.TaskInputsOutputsDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.util.ClientUtils;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleAssignmentsInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.RetryUtils;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.flink.config.RssFlinkConfig;
import org.apache.uniffle.flink.exception.ShuffleException;
import org.apache.uniffle.flink.resource.DefaultRssShuffleResource;
import org.apache.uniffle.flink.resource.RssShuffleResourceDescriptor;
import org.apache.uniffle.flink.utils.ShuffleUtils;

import static org.apache.uniffle.flink.config.RssFlinkConfig.RSS_HEARTBEAT_INTERVAL;
import static org.apache.uniffle.flink.config.RssFlinkConfig.RSS_HEARTBEAT_TIMEOUT;
import static org.apache.uniffle.flink.config.RssFlinkConfig.RSS_MEMORY_PER_INPUT_GATE;
import static org.apache.uniffle.flink.config.RssFlinkConfig.RSS_MEMORY_PER_RESULT_PARTITION;
import static org.apache.uniffle.flink.config.RssFlinkConfig.RSS_QUOTA_USER;

public class RssShuffleMaster implements ShuffleMaster<RssShuffleDescriptor> {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleMaster.class);

  //
  private final ShuffleMasterContext shuffleMasterContext;
  private final RssApplication rssFlinkApplication;

  //
  protected ShuffleWriteClient shuffleWriteClient;

  //
  private boolean heartbeatStarted = false;
  private final ScheduledExecutorService heartBeatScheduledExecutorService;

  //
  private final ScheduledExecutorService executorService;

  private final Configuration config;

  public RssShuffleMaster(ShuffleMasterContext shuffleMasterContext) {
    this.shuffleMasterContext = shuffleMasterContext;
    this.config = shuffleMasterContext.getConfiguration();
    this.rssFlinkApplication = new RssApplication();
    this.heartBeatScheduledExecutorService =
        ThreadUtils.getDaemonSingleThreadScheduledExecutor("uniffle-rss-heartbeat");
    this.executorService =
        ThreadUtils.getDaemonSingleThreadScheduledExecutor("uniffle-shuffle-master-executor");
  }

  @Override
  public void close() throws Exception {
    if (heartBeatScheduledExecutorService != null) {
      heartBeatScheduledExecutorService.shutdownNow();
    }
    if (executorService != null) {
      executorService.shutdownNow();
    }
    if (shuffleWriteClient != null) {
      shuffleWriteClient.close();
    }
  }

  /**
   * The ShuffleMaster is responsible for the allocation and release of resources. This method
   * serves two purposes: 1. It registers the job and the JobShuffleContext with the ShuffleMaster.
   * This registration mechanism also allows the ShuffleMaster to notify the JobMaster when result
   * partitions are lost, enabling the JobMaster to promptly identify and regenerate those
   * unavailable partitions. 2. We register the coordinators of the RSS service here.
   *
   * @param context the corresponding shuffle context of the target job.
   */
  @Override
  public void registerJob(JobShuffleContext context) {
    JobID jobId = context.getJobId();
    if (shuffleWriteClient == null) {
      synchronized (RssShuffleMaster.class) {
        // Step1. 获取applicationId
        String applicationId = rssFlinkApplication.registerApplicationId(jobId);
        LOG.info("Rss ApplicationId : {}.", applicationId);

        // Step2. 注册Coordinators
        String coordinators = config.getString(RssFlinkConfig.RSS_COORDINATOR_QUORM);
        LOG.info("Start Registering Coordinator {}.", coordinators);
        shuffleWriteClient =
            ShuffleUtils.createShuffleClient(shuffleMasterContext.getConfiguration());
        shuffleWriteClient.registerCoordinators(coordinators);
      }
    }
  }

  /**
   * Asynchronously register a partition and its producer with the shuffle service.
   *
   * <p>The returned shuffle descriptor is an internal handle which identifies the partition
   * internally within the shuffle service. The descriptor should provide enough information to read
   * from or write data to the partition.
   *
   * @param jobId job ID of the corresponding job which registered the partition
   * @param partitionDescriptor general job graph information about the partition
   * @param producerDescriptor general producer information (location, execution id, connection
   *     info)
   *     <p>ProducerDescriptor producerExecutionId -> The ID of the producer execution attempt.
   * @return future with the partition shuffle descriptor used for producer/consumer deployment and
   *     their data exchange.
   */
  @Override
  public CompletableFuture<RssShuffleDescriptor> registerPartitionWithProducer(
      JobID jobId, PartitionDescriptor partitionDescriptor, ProducerDescriptor producerDescriptor) {

    CompletableFuture<RssShuffleDescriptor> future = new CompletableFuture<>();

    executorService.execute(
        () -> {
          // Step1. genShuffleResourceDescriptor
          // 对于同一个IntermediateDataSet，ShuffleId要保持一致
          RssShuffleResourceDescriptor rssShuffleResourceDescriptor =
              rssFlinkApplication.getShuffleResourceDescriptor(jobId, partitionDescriptor);
          int shuffleId = rssShuffleResourceDescriptor.getShuffleId();

          // Step2. get basic parameters
          Configuration config = shuffleMasterContext.getConfiguration();
          Set<String> assignmentTags = ShuffleUtils.genAssignmentTags(config);
          int requiredShuffleServerNumber = ShuffleUtils.getRequiredShuffleServerNumber(config);
          String applicationId = rssFlinkApplication.getApplicationId();
          boolean dynamicClientConfEnabled =
              config.getBoolean(RssFlinkConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED);
          String remoteStoragePath = config.getString(RssFlinkConfig.RSS_REMOTE_STORAGE_PATH);
          RemoteStorageInfo defaultRemoteStorage = new RemoteStorageInfo(remoteStoragePath);
          String storageType = config.getString(RssFlinkConfig.RSS_STORAGE_TYPE);
          RemoteStorageInfo remoteStorage =
              ClientUtils.fetchRemoteStorage(
                  applicationId,
                  defaultRemoteStorage,
                  dynamicClientConfEnabled,
                  storageType,
                  shuffleWriteClient);
          long retryInterval = config.get(RssFlinkConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL);
          int retryTimes = config.get(RssFlinkConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES);

          // Step3. get partitionToServers.
          Map<Integer, List<ShuffleServerInfo>> partitionToServers;
          try {
            partitionToServers =
                RetryUtils.retry(
                    () -> {
                      ShuffleAssignmentsInfo response =
                          shuffleWriteClient.getShuffleAssignments(
                              applicationId,
                              shuffleId,
                              partitionDescriptor.getNumberOfSubpartitions(),
                              1,
                              assignmentTags,
                              requiredShuffleServerNumber,
                              -1);
                      registerShuffleServers(
                          applicationId,
                          shuffleId,
                          response.getServerToPartitionRanges(),
                          remoteStorage);
                      return response.getPartitionToServers();
                    },
                    retryInterval,
                    retryTimes);
          } catch (Throwable throwable) {
            throw new RssException("registerShuffle failed!", throwable);
          }

          // Step4. genDefaultRssShuffleResource
          DefaultRssShuffleResource resource =
              new DefaultRssShuffleResource(partitionToServers, rssShuffleResourceDescriptor);
          ResultPartitionID resultPartitionId =
              rssFlinkApplication.getResultPartitionId(partitionDescriptor, producerDescriptor);
          RssShuffleDescriptor rsd = new RssShuffleDescriptor(jobId, resultPartitionId, resource);
          future.complete(rsd);

          // Step5. startHeartbeat
          startHeartbeat(config);
        });

    return future;
  }

  protected void registerShuffleServers(
      String appId,
      int shuffleId,
      Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges,
      RemoteStorageInfo remoteStorage) {

    // ShuffleDataDistributionType dataDistributionType = getShuffleDataDistributionType(config);
    ShuffleDataDistributionType dataDistributionType = null;
    int maxConcurrencyPerPartitionToWrite =
        RssFlinkConfig.toRssConf(config)
            .getInteger(RssClientConf.MAX_CONCURRENCY_PER_PARTITION_TO_WRITE);

    if (serverToPartitionRanges == null || serverToPartitionRanges.isEmpty()) {
      return;
    }
    LOG.info("Start to register shuffleId[{}].", shuffleId);
    long start = System.currentTimeMillis();
    Set<Map.Entry<ShuffleServerInfo, List<PartitionRange>>> entries =
        serverToPartitionRanges.entrySet();
    entries.forEach(
        entry ->
            shuffleWriteClient.registerShuffle(
                entry.getKey(),
                appId,
                shuffleId,
                entry.getValue(),
                remoteStorage,
                dataDistributionType,
                maxConcurrencyPerPartitionToWrite));
    LOG.info(
        "Finish register shuffleId[{}] with {} ms.",
        shuffleId,
        ((System.currentTimeMillis() - start)));
  }

  private synchronized void startHeartbeat(Configuration config) {
    String user = config.getString(RSS_QUOTA_USER);
    long heartbeatInterval = config.getLong(RSS_HEARTBEAT_INTERVAL);
    long heartbeatTimeout = config.getLong(RSS_HEARTBEAT_TIMEOUT, heartbeatInterval / 2);
    String uniffleAppId = rssFlinkApplication.getApplicationId();

    shuffleWriteClient.registerApplicationInfo(uniffleAppId, heartbeatTimeout, user);
    if (!heartbeatStarted) {
      heartBeatScheduledExecutorService.scheduleAtFixedRate(
          () -> {
            try {
              shuffleWriteClient.sendAppHeartbeat(uniffleAppId, heartbeatTimeout);
              LOG.info("Finish send heartbeat to coordinator and servers");
            } catch (Exception e) {
              LOG.warn("Fail to send heartbeat to coordinator and servers", e);
            }
          },
          heartbeatInterval / 2,
          heartbeatInterval,
          TimeUnit.MILLISECONDS);
      heartbeatStarted = true;
    }
  }

  /**
   * Release any external resources occupied by the given partition.
   *
   * <p>This call triggers release of any resources which are occupied by the given partition in the
   * external systems outside of the producer executor. This is mostly relevant for the batch jobs
   * and blocking result partitions. The producer local resources are managed by {@link
   * ShuffleDescriptor#storesLocalResourcesOn()} and {@link
   * ShuffleEnvironment#releasePartitionsLocally(Collection)}.
   *
   * @param shuffleDescriptor shuffle descriptor of the result partition to release externally.
   */
  @Override
  public void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor) {
    executorService.execute(
        () -> {
          if (!(shuffleDescriptor instanceof RssShuffleDescriptor)) {
            LOG.error(
                "Rss only supports RssShuffleDescriptor and does not support {}.",
                shuffleDescriptor.getClass().getName());
            shuffleMasterContext.onFatalError(
                new ShuffleException(
                    shuffleDescriptor.getClass().getName() + " is an unsupported type."));
            return;
          }

          RssShuffleDescriptor descriptor = (RssShuffleDescriptor) shuffleDescriptor;
          try {
            String applicationId = rssFlinkApplication.getApplicationId();
            int rssTaskShuffleId = rssFlinkApplication.getRssTaskShuffleId(descriptor);

            if (shuffleWriteClient != null) {
              shuffleWriteClient.unregisterShuffle(applicationId, rssTaskShuffleId);
            }
          } catch (Throwable throwable) {
            LOG.error(
                "releasePartitionExternally error, shuffleDescriptor = {}.", descriptor, throwable);
          }
        });
  }

  /**
   * Compute shuffle memory size for a task with the given {@link TaskInputsOutputsDescriptor}.
   *
   * @param taskInputsOutputsDescriptor describes task inputs and outputs information for shuffle
   *     memory calculation.
   * @return shuffle memory size for a task with the given {@link TaskInputsOutputsDescriptor}.
   */
  @Override
  public MemorySize computeShuffleMemorySizeForTask(
      TaskInputsOutputsDescriptor taskInputsOutputsDescriptor) {
    for (ResultPartitionType partitionType :
        taskInputsOutputsDescriptor.getPartitionTypes().values()) {
      if (!partitionType.isBlocking()) {
        throw new ShuffleException(
            "Blocking result partition type expected but found " + partitionType);
      }
    }

    int numResultPartitions = taskInputsOutputsDescriptor.getSubpartitionNums().size();
    long numBytesPerPartition = config.get(RSS_MEMORY_PER_RESULT_PARTITION).getBytes();
    long numBytesForOutput = numBytesPerPartition * numResultPartitions;

    int numInputGates = taskInputsOutputsDescriptor.getInputChannelNums().size();
    long numBytesPerGate = config.get(RSS_MEMORY_PER_INPUT_GATE).getBytes();
    long numBytesForInput = numBytesPerGate * numInputGates;

    return new MemorySize(numBytesForInput + numBytesForOutput);
  }
}
