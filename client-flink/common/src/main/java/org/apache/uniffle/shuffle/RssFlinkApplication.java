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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;

import org.apache.uniffle.shuffle.resource.RssShuffleResourceDescriptor;

public class RssFlinkApplication {

  private String uniffleApplicationId;
  private String flinkShuffleId;

  ////////////////////////////////
  // FlinkId2UniffleId mapping relationship
  ////////////////////////////////

  // When we implemented Uniffle Flink, Uniffle already fully supported the related functions of MR
  // and Spark.
  // In order to be compatible with this part of the code, we need to map Flink's Id.
  private final Map<String, Integer> flinkShuffleId2UniffleId = new ConcurrentHashMap<>();
  private final Map<Integer, String> uniffleShuffleId2FlinkShuffleId = new ConcurrentHashMap<>();
  private final Map<Integer, Integer> uniffleShuffleId2PartitionId = new ConcurrentHashMap<>();
  private final Map<Integer, Map<Integer, AtomicInteger>> uniffleShuffleIdMapIdAttemptId =
      new ConcurrentHashMap<>();
  private AtomicInteger shuffleIndex = new AtomicInteger(0);

  public RssFlinkApplication() {}

  public String genUniffleApplicationId(JobID jobId) {
    if (StringUtils.isNotBlank(uniffleApplicationId)) {
      return uniffleApplicationId;
    }
    uniffleApplicationId = "uniffle_" + jobId.toString();
    return uniffleApplicationId;
  }

  /**
   * Generate ShuffleResourceDescriptor based on jobId, partitionDescriptor, producerDescriptor.
   *
   * @param jobId Unique (at least statistically unique) identifier for a Flink Job.
   * @param partitionDescriptor Partition descriptor for {@link ShuffleMaster} to obtain {@link
   *     ShuffleDescriptor}.
   * @return RssShuffleResourceDescriptor.
   */
  public RssShuffleResourceDescriptor genShuffleResourceDescriptor(
      JobID jobId, PartitionDescriptor partitionDescriptor) {
    // Step1. Generate Flink ShuffleId.
    this.flinkShuffleId = getFlinkShuffleId(jobId, partitionDescriptor);

    // Step2. Generate Uniffle ShuffleId\Uniffle PartitionId\Uniffle mapPartitionId\Uniffle
    // AttemptId.
    int uniffleShuffleId = getUniffleShuffleId(flinkShuffleId);
    int unifflePartitionId = getUnifflePartitionId(uniffleShuffleId);
    int mapPartitionId = partitionDescriptor.getPartitionId().getPartitionNumber();
    int uniffleAttemptId = genUniffleAttemptId(uniffleShuffleId, mapPartitionId);

    // Step3. Generate RssShuffleResourceDescriptor.
    return new RssShuffleResourceDescriptor(
        uniffleShuffleId, mapPartitionId, uniffleAttemptId, unifflePartitionId);
  }

  /**
   * Get Flink ShuffleId. We will concatenate jobId and dataSetId together.
   *
   * @param jobId Unique (at least statistically unique) identifier for a Flink Job.
   * @param partitionDescriptor Partition descriptor for {@link ShuffleMaster} to obtain {@link
   *     ShuffleDescriptor}.
   * @return Flink ShuffleId.
   */
  public String getFlinkShuffleId(JobID jobId, PartitionDescriptor partitionDescriptor) {
    IntermediateDataSetID dataSetId = partitionDescriptor.getResultId();
    return jobId.toString() + "_" + dataSetId.toString();
  }

  /**
   * Get Uniffle ShuffleId.
   *
   * @param shuffleId flink shuffle id
   * @return Uniffle ShuffleId, Value of type Int.
   */
  public int getUniffleShuffleId(String shuffleId) {
    // Ensure that under concurrent requests, the data meets expectations.
    synchronized (flinkShuffleId2UniffleId) {
      // If the data exists, we will return directly.
      if (flinkShuffleId2UniffleId.containsKey(shuffleId)) {
        return flinkShuffleId2UniffleId.get(shuffleId);
      }
      int newUniffleShuffleId = shuffleIndex.intValue();
      flinkShuffleId2UniffleId.put(shuffleId, newUniffleShuffleId);
      uniffleShuffleId2FlinkShuffleId.put(newUniffleShuffleId, shuffleId);
      uniffleShuffleIdMapIdAttemptId.put(newUniffleShuffleId, new ConcurrentHashMap<>());
      uniffleShuffleId2PartitionId.put(newUniffleShuffleId, 0);
      return shuffleIndex.getAndIncrement();
    }
  }

  /**
   * Generate Uniffle PartitionId.
   *
   * @param uniffleShuffleId Uniffle ShuffleId.
   * @return Uniffle PartitionId.
   */
  private int getUnifflePartitionId(int uniffleShuffleId) {
    synchronized (uniffleShuffleId2PartitionId) {
      int partitionId = uniffleShuffleId2PartitionId.getOrDefault(uniffleShuffleId, 0);
      uniffleShuffleId2PartitionId.put(uniffleShuffleId, partitionId + 1);
      return partitionId;
    }
  }

  /**
   * Generate Uniffle AttemptId.
   *
   * @param uniffleShuffleId Uniffle ShuffleId.
   * @param mapPartitionId Flink Map PartitionId.
   * @return Uniffle AttemptId.
   */
  private int genUniffleAttemptId(int uniffleShuffleId, int mapPartitionId) {
    synchronized (uniffleShuffleIdMapIdAttemptId) {
      Map<Integer, AtomicInteger> shuffleIdMapIdAttemptId =
          uniffleShuffleIdMapIdAttemptId.getOrDefault(uniffleShuffleId, new ConcurrentHashMap<>());
      AtomicInteger attemptId =
          shuffleIdMapIdAttemptId.getOrDefault(mapPartitionId, new AtomicInteger(0));
      return attemptId.getAndIncrement();
    }
  }

  public String getUniffleApplicationId() {
    return uniffleApplicationId;
  }

  public String getFlinkShuffleId() {
    return flinkShuffleId;
  }

  public ResultPartitionID genResultPartitionId(
      PartitionDescriptor partitionDescriptor, ProducerDescriptor producerDescriptor) {
    return new ResultPartitionID(
        partitionDescriptor.getPartitionId(), producerDescriptor.getProducerExecutionId());
  }
}
