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

package org.apache.uniffle.cli;

import java.util.HashMap;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import org.apache.uniffle.api.AdminRestApi;
import org.apache.uniffle.common.Application;
import org.apache.uniffle.common.util.http.UniffleRestClient;
import org.apache.uniffle.common.web.resource.Response;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class AdminRestApiTest {

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private UniffleRestClient uniffleRestClient;

  @InjectMocks private AdminRestApi adminRestApi;
  private final ObjectMapper objectMapper = new ObjectMapper();

  @BeforeEach
  public void setup() throws Exception {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testRunRefreshAccessChecker() {
    Mockito.when(
            uniffleRestClient
                .getHttpClient()
                .get(Mockito.anyString(), Mockito.anyMap(), Mockito.any()))
        .thenReturn("OK");
    adminRestApi.refreshAccessChecker();
    Mockito.verify(uniffleRestClient.getHttpClient(), Mockito.times(1))
        .get("/api/admin/refreshChecker", new HashMap<>(), null);
  }

  @Test
  public void testGetApplications() throws Exception {
    List<Application> applications =
        Lists.newArrayList(new Application.Builder().applicationId("1").build());
    Mockito.when(
            adminRestApi.getClient().post(Mockito.anyString(), Mockito.anyMap(), Mockito.any()))
        .thenReturn(objectMapper.writeValueAsString(Response.success(applications)));
    List<Application> applicationRes = adminRestApi.getApplications("", "", "", "", "");
    assertTrue(CollectionUtils.isEqualCollection(applications, applicationRes));
  }
}
