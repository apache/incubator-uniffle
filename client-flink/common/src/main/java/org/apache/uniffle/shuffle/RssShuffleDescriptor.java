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

package org.apache.uniffle.shuffle;

import java.util.Optional;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;

import org.apache.uniffle.shuffle.resource.DefaultRssShuffleResource;

/**
 * 1. put into ResultPartitionDeploymentDescriptor for submitting producer task 2. as a known
 * producer inside InputGateDeploymentDescriptor for submitting consumer task. 3. It can contain
 * specific partition config for ShuffleEnvironment on TE side to serve partition writer and reader.
 */
public class RssShuffleDescriptor implements ShuffleDescriptor {

  private final ResultPartitionID resultPartitionID;

  private final JobID jobId;

  private final DefaultRssShuffleResource shuffleResource;

  public RssShuffleDescriptor(
      ResultPartitionID resultPartitionID, JobID jobId, DefaultRssShuffleResource shuffleResource) {
    this.resultPartitionID = resultPartitionID;
    this.jobId = jobId;
    this.shuffleResource = shuffleResource;
  }

  @Override
  public ResultPartitionID getResultPartitionID() {
    return resultPartitionID;
  }

  @Override
  public Optional<ResourceID> storesLocalResourcesOn() {
    return Optional.empty();
  }

  public JobID getJobId() {
    return jobId;
  }

  public DefaultRssShuffleResource getShuffleResource() {
    return shuffleResource;
  }
}
