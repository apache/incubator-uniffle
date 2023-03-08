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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;

import org.apache.spark.MapOutputTracker;
import org.apache.spark.MapOutputTrackerMaster;
import org.apache.spark.SparkEnv;
import org.apache.spark.SparkException;
import org.apache.spark.shuffle.RssFeatureMatrix;
import org.apache.spark.shuffle.SparkVersionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractRssShuffleManagerBase implements RssShuffleManagerBase {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractRssShuffleManagerBase.class);
  private static final Method unregisterAllMapOutputMethod = getUnregisterAllMapOutputMethod();

  @Override
  public void unregisterAllMapOutput(int shuffleId) throws SparkException {
    if (!RssFeatureMatrix.isStageRecomputeSupported()) {
      return;
    }
    if (unregisterAllMapOutputMethod != null) {
      try {
        unregisterAllMapOutputMethod.invoke(getMapOutputTrackerMaster(), shuffleId);
      } catch (InvocationTargetException | IllegalAccessException e) {
        throw new SparkException("Invoke unregisterAllMapOutput method failed", e);
      }
    } else {
      defaultUnregisterAllMapOutput(shuffleId, getPartitionNum(shuffleId));
    }
  }

  private static void defaultUnregisterAllMapOutput(int shuffleId, int partitionNum) throws SparkException {
    MapOutputTrackerMaster tracker = getMapOutputTrackerMaster();
    if (tracker != null) {
      tracker.unregisterShuffle(shuffleId);
      // re-register this shuffle id into map output tracker
      tracker.registerShuffle(shuffleId, partitionNum);
      tracker.incrementEpoch();
    } else {
      throw new SparkException("default unregisterAllMapOutput should only be called on the driver side");
    }
  }

  private static Method getUnregisterAllMapOutputMethod() {
    if (getMapOutputTrackerMaster() == null) {
      Class<? extends MapOutputTrackerMaster> klass = getMapOutputTrackerMaster().getClass();
      Method m = null;
      try {
        if (SparkVersionUtils.isSpark2() && SparkVersionUtils.minorVersion <= 3) {
          // for spark version less than 2.3, there's no unregisterAllMapOutput support
          LOG.warn("Spark version <= 2.3, fallback to default method");
        } else if (SparkVersionUtils.isSpark2()) {
          // this method is added in Spark 2.4+
          m = klass.getDeclaredMethod("unregisterAllMapOutput", int.class);
        } else if (SparkVersionUtils.isSpark3() && SparkVersionUtils.minorVersion <= 1) {
          // spark 3.1 will have unregisterAllMapOutput method
          m = klass.getDeclaredMethod("unregisterAllMapOutput", int.class);
        } else if (SparkVersionUtils.isSpark3()) {
          m = klass.getDeclaredMethod("unregisterAllMapAndMergeOutput", int.class);
        } else {
          LOG.warn("Unknown spark version({}), fallback to default method", SparkVersionUtils.sparkVersion);
        }
      } catch (NoSuchMethodException e) {
        LOG.warn("Got no such method error when get unregisterAllMapOutput method for spark version({})",
            SparkVersionUtils.sparkVersion);
      }
      return m;
    } else {
      return null;
    }
  }

  private static MapOutputTrackerMaster getMapOutputTrackerMaster() {
    MapOutputTracker tracker =  Optional.ofNullable(SparkEnv.get()).map(SparkEnv::mapOutputTracker).orElse(null);
    return tracker instanceof MapOutputTrackerMaster ? (MapOutputTrackerMaster) tracker : null;
  }
}
