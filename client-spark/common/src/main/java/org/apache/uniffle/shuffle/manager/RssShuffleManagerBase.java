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

package org.apache.uniffle.shuffle.manager;

import org.apache.spark.SparkException;

// proxy interface to delegate shuffle un-registration operation to MapOutputTracker
public interface RssShuffleManagerBase {

  /**
   * @return the unique spark id for rss shuffle
   */
  String getAppId();

  /**
   * @return the maximum number of fetch failures per shuffle partition before that shuffle stage should be recomputed
   */
  int getMaxFetchFailures();

  /**
   * @param shuffleId the shuffleId to query
   * @return the num of partitions(a.k.a reduce tasks) for shuffle with shuffle id.
   */
  int getPartitionNum(int shuffleId);

  int getNumMapTasks(int shuffleId);

  /**
   * Unregister all the map output on the driver side, so the whole stage could be re-computed.
   * @param shuffleId the shuffle id to unregister
   * @throws SparkException
   */
  void unregisterAllMapOutput(int shuffleId) throws SparkException;
}
