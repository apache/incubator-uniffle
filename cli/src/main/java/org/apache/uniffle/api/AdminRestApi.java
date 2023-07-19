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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;

import org.apache.uniffle.client.RestClient;
import org.apache.uniffle.client.UniffleRestClient;
import org.apache.uniffle.coordinator.Application;
import org.apache.uniffle.entity.ApplicationResponse;

public class AdminRestApi {
  private UniffleRestClient client;

  private AdminRestApi() {}

  public AdminRestApi(UniffleRestClient client) {
    this.client = client;
  }

  public String refreshAccessChecker() {
    Map<String, Object> params = new HashMap<>();
    return this.getClient().get("/api/admin/refreshChecker", params, null);
  }

  public List<Application> getApplications(String applications) throws JsonProcessingException {
    List<Application> results = new ArrayList<>();
    String postJson = getApplicationsJson(applications);
    if (StringUtils.isNotBlank(postJson)) {
      ObjectMapper objectMapper = new ObjectMapper();
      ApplicationResponse response = objectMapper.readValue(postJson,  new TypeReference<ApplicationResponse>() {});
      if (response != null && response.getData() != null) {
        results.addAll(response.getData());
      }
    }
    return results;
  }

  public String getApplicationsJson(String applications) {
    Map<String, Object> params = new HashMap<>();
    String[] applicationArrays = applications.split(",");
    params.put("applications", applicationArrays);
    return this.getClient().post("/api/server/applications",  params, null);
  }

  private RestClient getClient() {
    return this.client.getHttpClient();
  }
}
