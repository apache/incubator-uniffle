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

public class RssFeatureMatrix {
  public static boolean isStageRecomputeSupported() {
    // Stage re-computation requires the ShuffleMapTask to throw a FetchFailedException, which would be produced by the
    // shuffle data reader iterator. However, the shuffle reader iterator interface is defined in Scala, which doesn't
    // have checked exceptions. This makes it hard to throw a FetchFailedException on the Java side.
    // Fortunately, starting from Spark 2.3 (or maybe even Spark 2.2), it is possible to create a FetchFailedException
    // and wrap it into a runtime exception. Spark will consider this exception as a FetchFailedException.
    // Therefore, the stage re-computation feature is only enabled for Spark versions larger than or equal to 2.3.
    return SparkVersionUtils.isSpark3() || (SparkVersionUtils.isSpark2() && SparkVersionUtils.MINOR_VERSION >= 3);
  }
}
