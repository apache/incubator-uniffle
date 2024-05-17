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

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;

import org.apache.uniffle.flink.resource.RssShuffleResourceDescriptor;

/**
 * 这是FlinkApplication在RSS中的抽象，包括2部分内容： 1. 一个String类型的applicationId，这就是Flink的Job.toString得到的。 2.
 * RssApplicationStateStore，用于存储
 */
public class RssApplication {

  private String applicationId;
  private final RssApplicationStateStore stateStore;

  public RssApplication() {
    stateStore = new RssApplicationStateStore();
  }

  public String registerApplicationId(JobID jobId) {
    if (StringUtils.isNotBlank(applicationId)) {
      return applicationId;
    }
    applicationId = jobId.toString();
    return applicationId;
  }

  public String getApplicationId() {
    return applicationId;
  }

  public RssShuffleResourceDescriptor getShuffleResourceDescriptor(
      JobID jobId, PartitionDescriptor partitionDescriptor) {

    // Step1. Generate Task ShuffleId.
    IntermediateDataSetID dataSetId = partitionDescriptor.getResultId();
    String taskShuffleId = getTaskShuffleId(jobId, dataSetId);
    int shuffleId = stateStore.getRssTaskShuffleId(taskShuffleId);

    // Step2. Generate Task PartitionId.
    // jobId, taskShuffleId
    IntermediateResultPartitionID partitionId = partitionDescriptor.getPartitionId();
    int rssPartitionId = stateStore.getRssPartitionId(taskShuffleId, dataSetId.toString());
    int mapPartitionId = partitionId.getPartitionNumber();

    // Step3. Generate Task PartitionId
    int rssAttemptId = stateStore.getRssPartitionId(taskShuffleId, dataSetId.toString());

    return new RssShuffleResourceDescriptor(
        shuffleId, rssPartitionId, mapPartitionId, rssAttemptId);
  }

  public String getTaskShuffleId(JobID jobId, IntermediateDataSetID dataSetId) {
    return jobId.toString() + "_" + dataSetId.toString();
  }

  public ResultPartitionID getResultPartitionId(
      PartitionDescriptor partitionDescriptor, ProducerDescriptor producerDescriptor) {
    return new ResultPartitionID(
        partitionDescriptor.getPartitionId(), producerDescriptor.getProducerExecutionId());
  }

  public int getRssTaskShuffleId(RssShuffleDescriptor descriptor) {
    assert descriptor != null;
    JobID jobId = descriptor.getJobId();
    ResultPartitionID resultPartitionId = descriptor.getResultPartitionID();
    IntermediateResultPartitionID partitionId = resultPartitionId.getPartitionId();
    IntermediateDataSetID intermediateDataSetId = partitionId.getIntermediateDataSetID();
    String taskShuffleId = getTaskShuffleId(jobId, intermediateDataSetId);
    return stateStore.getUnknowRssTaskShuffleId(taskShuffleId);
  }
}
