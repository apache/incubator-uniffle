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

package org.apache.spark.shuffle;

import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleDriverComponents;

public class RssShuffleDriverComponents extends LocalDiskShuffleDriverComponents {

  private final SparkConf sparkConf;

  public RssShuffleDriverComponents(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
  }

  /**
   * Omitting @Override annotation to avoid compile error before Spark 3.5.0
   *
   * <p>This method is called after DelegationRssShuffleManager initialize, so
   * RssSparkConfig.RSS_ENABLED must be already set
   */
  public boolean supportsReliableStorage() {
    return sparkConf.get(RssSparkConfig.RSS_ENABLED)
        || RssShuffleManager.class
            .getCanonicalName()
            .equals(sparkConf.get("spark.shuffle.manager"));
  }
}
