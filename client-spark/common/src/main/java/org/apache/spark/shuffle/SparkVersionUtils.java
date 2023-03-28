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
  public final static String sparkVersion = package$.MODULE$.SPARK_VERSION();
  public final static int majorVersion;
  public final static int minorVersion;

  static {
    int _majorVersion;
    int _minorVersion;
    try {
      _majorVersion = VersionUtils.majorVersion(sparkVersion);
      _minorVersion = VersionUtils.minorVersion(sparkVersion);
    } catch (Exception e) {
      // ignore, just in case some wild spark version is passed.
      _majorVersion = -1;
      _minorVersion = -1;
    }
    majorVersion = _majorVersion;
    minorVersion = _minorVersion;
  }


  public static boolean isSpark2() {
    return majorVersion == 2;
  }

  public static boolean isSpark3() {
    return majorVersion == 3;
  }

}
