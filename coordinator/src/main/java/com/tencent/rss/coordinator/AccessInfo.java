/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.rss.coordinator;

import java.util.Set;

import com.google.common.collect.Sets;

public class AccessInfo {
  private final String accessId;
  private final Set<String> tags;

  public AccessInfo(String accessId, Set<String> tags) {
    this.accessId = accessId;
    this.tags = tags;
  }

  public AccessInfo(String accessId) {
    this(accessId, Sets.newHashSet());
  }

  public String getAccessId() {
    return accessId;
  }

  public Set<String> getTags() {
    return tags;
  }

  @Override
  public String toString() {
    return "AccessInfo{"
        + "accessId='" + accessId + '\''
        + ", tags=" + tags
        + '}';
  }
}
