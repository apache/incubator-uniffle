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

package org.apache.uniffle.common;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.time.DateFormatUtils;

public class Application implements Comparable<Application> {

  private static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";
  private String applicationId;
  private String user;
  private String lastHeartBeatTime;
  private String remoteStoragePath;
  private String registerTime;

  public Application() {}

  public Application(Builder builder) {
    this.applicationId = builder.applicationId;
    this.user = builder.user;
    this.lastHeartBeatTime = builder.lastHeartBeatTime;
    this.remoteStoragePath = builder.remoteStoragePath;
    this.registerTime = builder.registerTime;
  }

  public static class Builder {
    private String applicationId;
    private String user;
    private String lastHeartBeatTime;
    private String registerTime;
    private String remoteStoragePath;

    public Builder() {}

    public Builder applicationId(String applicationId) {
      this.applicationId = applicationId;
      return this;
    }

    public Builder user(String user) {
      this.user = user;
      return this;
    }

    public Builder lastHeartBeatTime(long lastHeartBeatTime) {
      this.lastHeartBeatTime = DateFormatUtils.format(lastHeartBeatTime, DATE_PATTERN);
      return this;
    }

    public Builder registerTime(long registerTime) {
      this.registerTime = DateFormatUtils.format(registerTime, DATE_PATTERN);
      return this;
    }

    public Builder remoteStoragePath(RemoteStorageInfo remoteStorageInfo) {
      if (remoteStorageInfo != null) {
        this.remoteStoragePath = remoteStorageInfo.getPath();
      }
      return this;
    }

    public Application build() {
      return new Application(this);
    }
  }

  public String getApplicationId() {
    return applicationId;
  }

  public String getUser() {
    return user;
  }

  public String getLastHeartBeatTime() {
    return lastHeartBeatTime;
  }

  public String getRegisterTime() {
    return registerTime;
  }

  public String getRemoteStoragePath() {
    return remoteStoragePath;
  }

  public void setApplicationId(String applicationId) {
    this.applicationId = applicationId;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public void setLastHeartBeatTime(String lastHeartBeatTime) {
    this.lastHeartBeatTime = lastHeartBeatTime;
  }

  public void setRegisterTime(String registerTime) {
    this.registerTime = registerTime;
  }

  public void setRemoteStoragePath(String remoteStoragePath) {
    this.remoteStoragePath = remoteStoragePath;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Application)) {
      return false;
    }
    Application otherImpl = this.getClass().cast(other);
    return new EqualsBuilder()
        .append(this.getApplicationId(), otherImpl.getApplicationId())
        .append(this.getUser(), otherImpl.getUser())
        .append(this.getLastHeartBeatTime(), otherImpl.getLastHeartBeatTime())
        .append(this.getRegisterTime(), otherImpl.getRegisterTime())
        .append(this.getRemoteStoragePath(), otherImpl.getRemoteStoragePath())
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(this.getApplicationId())
        .append(this.getUser())
        .append(this.getLastHeartBeatTime())
        .append(this.getRegisterTime())
        .append(this.getRemoteStoragePath())
        .toHashCode();
  }

  @Override
  public int compareTo(Application other) {
    return this.applicationId.compareTo(other.applicationId);
  }

  @Override
  public String toString() {
    return "Application{"
        + "applicationId='"
        + applicationId
        + '\''
        + ", user='"
        + user
        + '\''
        + ", lastHeartBeatTime='"
        + lastHeartBeatTime
        + '\''
        + ", registerTime='"
        + registerTime
        + '\''
        + ", remoteStoragePath='"
        + remoteStoragePath
        + '\''
        + '}';
  }
}
