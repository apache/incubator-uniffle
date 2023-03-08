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

import org.apache.spark.package$;
import org.apache.spark.util.VersionUtils;

public class SparkVersionUtils {
  public static String sparkVersion = package$.MODULE$.SPARK_VERSION();
  public static int majorVersion = -1;
  public static int minorVersion = -1;

  static {
    try {
      majorVersion =  VersionUtils.majorVersion(sparkVersion);
      minorVersion = VersionUtils.minorVersion(sparkVersion);
    } catch (Exception e) {
      // ignore, just in case some wild spark version is passed.
    }
  }


  public static boolean isSpark2() {
    return majorVersion == 2;
  }

  public static boolean isSpark3() {
    return majorVersion == 3;
  }

}
