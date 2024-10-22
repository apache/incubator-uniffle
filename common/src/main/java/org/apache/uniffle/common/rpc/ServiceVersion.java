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

package org.apache.uniffle.common.rpc;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class ServiceVersion {
  public static final ServiceVersion NEWEST_VERSION =
      new ServiceVersion(Feature.getNewest().getVersion());
  static final Map<Integer, Feature> VALUE_MAP =
      Arrays.stream(Feature.values()).collect(Collectors.toMap(Feature::getVersion, s -> s));
  private final int version;

  public ServiceVersion(int version) {
    this.version = version;
  }

  public Feature getCurrentFeature() {
    return VALUE_MAP.get(version);
  }

  public boolean supportFeature(Feature registerBlockIdLayout) {
    return version >= registerBlockIdLayout.getVersion();
  }

  public int getVersion() {
    return version;
  }

  public enum Feature {
    // Treat the old version as init version
    INIT_VERSION(0),
    // Register block id layout to server to avoid sending block id layout for each getShuffleResult
    // request
    REGISTER_BLOCK_ID_LAYOUT(1),
    ;

    private final int version;

    Feature(int version) {
      this.version = version;
    }

    public int getVersion() {
      return version;
    }

    public static Feature getNewest() {
      Feature[] enumConstants = Feature.class.getEnumConstants();
      return enumConstants[enumConstants.length - 1];
    }
  }
}
