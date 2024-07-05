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

package org.apache.uniffle.common.exception;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.uniffle.common.ShuffleServerInfo;

/** Dedicated exception for rss client's shuffle failed related exception. */
public class RssFetchFailedException extends RssException {
  private Set<ShuffleServerInfo> fetchFailureServerIds = new HashSet<>();

  public RssFetchFailedException(String message, ShuffleServerInfo... fetchFailureServerIds) {
    super(message);
    Arrays.stream(fetchFailureServerIds).forEach(x -> this.fetchFailureServerIds.add(x));
  }

  public RssFetchFailedException(String message) {
    super(message);
  }

  public RssFetchFailedException(String message, Throwable e) {
    super(message, e);
  }

  public RssFetchFailedException(
      String message, Throwable e, ShuffleServerInfo... fetchFailureServerIds) {
    super(message, e);
    Arrays.stream(fetchFailureServerIds).forEach(x -> this.fetchFailureServerIds.add(x));
  }

  public Set<ShuffleServerInfo> getFetchFailureServerIds() {
    return fetchFailureServerIds;
  }
}
