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

package org.apache.uniffle.coordinator.web.request;

import java.util.Set;

import org.apache.commons.lang3.StringUtils;

/** This class is mainly used for Application requests. */
public class ApplicationRequest {
  private Set<String> applications;
  private int pageSize = 10;
  private int currentPage = 1;
  private String heartBeatStartTime;
  private String heartBeatEndTime;
  private String appIdRegex;

  public Set<String> getApplications() {
    return applications;
  }

  public void setApplications(Set<String> applications) {
    this.applications = applications;
  }

  public int getPageSize() {
    return pageSize;
  }

  public void setPageSize(int pageSize) {
    this.pageSize = pageSize;
  }

  public int getCurrentPage() {
    return currentPage;
  }

  public void setCurrentPage(int currentPage) {
    this.currentPage = currentPage;
  }

  public String getHeartBeatStartTime() {
    return heartBeatStartTime;
  }

  public void setHeartBeatStartTime(String heartBeatStartTime) {
    this.heartBeatStartTime = heartBeatStartTime;
  }

  public String getHeartBeatEndTime() {
    return heartBeatEndTime;
  }

  public void setHeartBeatEndTime(String heartBeatEndTime) {
    this.heartBeatEndTime = heartBeatEndTime;
  }

  public String getAppIdRegex() {
    return appIdRegex;
  }

  public void setAppIdRegex(String appIdRegex) {
    this.appIdRegex = appIdRegex;
  }

  @Override
  public String toString() {
    return "ApplicationRequest{applications=" + StringUtils.join(applications, ",") + '}';
  }
}
