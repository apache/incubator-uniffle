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

package org.apache.uniffle.coordinator.web.vo;

import java.util.Objects;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Builder
@AllArgsConstructor
@Getter
@Setter
public class AppInfoVO implements Comparable<AppInfoVO> {
  private String userName;
  private String appId;
  private long updateTime;
  private long registrationTime;
  private String version;
  private String gitCommitId;

  private long partitionNum;
  private long memorySize;
  private long localFileNum;
  private long localTotalSize;
  private long hadoopFileNum;
  private long hadoopTotalSize;
  private long totalSize;

  public AppInfoVO() {}

  public AppInfoVO(
      String userName,
      String appId,
      long updateTime,
      long registrationTime,
      String version,
      String gitCommitId) {
    this(userName, appId, updateTime, registrationTime, version, gitCommitId, 0, 0, 0, 0, 0, 0, 0);
  }

  @Override
  public int compareTo(AppInfoVO appInfoVO) {
    return Long.compare(registrationTime, appInfoVO.getRegistrationTime());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AppInfoVO)) {
      return false;
    }
    AppInfoVO appInfoVO = (AppInfoVO) o;
    return updateTime == appInfoVO.updateTime
        && registrationTime == appInfoVO.registrationTime
        && userName.equals(appInfoVO.userName)
        && appId.equals(appInfoVO.appId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(userName, appId, updateTime, registrationTime);
  }
}
