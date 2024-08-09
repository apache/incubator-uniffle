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

package org.apache.uniffle.api;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.Application;
import org.apache.uniffle.common.util.http.RestClient;
import org.apache.uniffle.common.util.http.UniffleRestClient;
import org.apache.uniffle.common.web.resource.Response;

public class AdminRestApi {

  private static final Logger LOG = LoggerFactory.getLogger(AdminRestApi.class);
  private UniffleRestClient client;
  private final ObjectMapper objectMapper = new ObjectMapper();

  private AdminRestApi() {}

  public AdminRestApi(UniffleRestClient client) {
    this.client = client;
  }

  public String refreshAccessChecker() {
    Map<String, Object> params = new HashMap<>();
    return this.getClient().get("/api/admin/refreshChecker", params, null);
  }

  public List<Application> getApplications(
      String applications,
      String applicationIdRegex,
      String pageSize,
      String currentPage,
      String heartBeatTimeRange)
      throws JsonProcessingException {
    List<Application> results = new ArrayList<>();
    String postJson =
        getApplicationsJson(
            applications, applicationIdRegex, pageSize, currentPage, heartBeatTimeRange);
    if (StringUtils.isNotBlank(postJson)) {
      Response<List<Application>> response =
          objectMapper.readValue(postJson, new TypeReference<Response<List<Application>>>() {});
      if (response != null && response.getData() != null) {
        results.addAll(response.getData());
      }
    }
    return results;
  }

  public String getApplicationsJson(
      String applications,
      String applicationIdRegex,
      String pageSize,
      String currentPage,
      String heartBeatTimeRange) {
    Map<String, Object> params = new HashMap<>();

    if (StringUtils.isNotBlank(applications)) {
      String[] applicationArrays = applications.split(",");
      params.put("applications", applicationArrays);
    }

    if (StringUtils.isNotBlank(applicationIdRegex)) {
      params.put("appIdRegex", applicationIdRegex);
    }

    if (StringUtils.isNotBlank(pageSize)) {
      params.put("pageSize", Integer.valueOf(pageSize));
    }

    if (StringUtils.isNotBlank(currentPage)) {
      params.put("currentPage", Integer.valueOf(currentPage));
    }

    if (StringUtils.isNotBlank(heartBeatTimeRange)) {
      transform(heartBeatTimeRange, params);
    }

    return this.getClient().post("/api/server/applications", params, null);
  }

  private void transform(String heartBeatTimeRange, Map<String, Object> params) {
    String[] timeRange = heartBeatTimeRange.split(",");
    String startTimeStr = null;
    String endTimeStr = null;

    if (timeRange.length == 2) {
      startTimeStr = timeRange[0];
      endTimeStr = timeRange[1];
    } else if (timeRange.length == 1) {
      startTimeStr = timeRange[0];
    }

    try {
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
      LocalDateTime startTime = null;
      LocalDateTime endTime = null;

      if (startTimeStr != null) {
        startTime = LocalDateTime.parse(startTimeStr.trim(), formatter);
      }

      if (endTimeStr != null) {
        endTime = LocalDateTime.parse(endTimeStr.trim(), formatter);
      }

      if (startTime != null && endTime != null) {
        long startTimeMillis = startTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        long endTimeMillis = endTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        params.put("heartBeatStartTime", startTimeMillis);
        params.put("heartBeatEndTime", endTimeMillis);
      } else if (startTime != null) {
        long startTimeMillis = startTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        params.put("heartBeatStartTime", startTimeMillis);
      } else if (endTime != null) {
        long endTimeMillis = endTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        params.put("heartBeatEndTime", endTimeMillis);
      }
    } catch (Exception e) {
      LOG.error("transform heartBeatTimeRange error.", e);
    }
  }

  public RestClient getClient() {
    return this.client.getHttpClient();
  }
}
