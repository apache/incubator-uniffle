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

package org.apache.uniffle.coordinator.web.servlet;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.coordinator.Application;
import org.apache.uniffle.coordinator.ApplicationManager;
import org.apache.uniffle.coordinator.CoordinatorServer;
import org.apache.uniffle.coordinator.web.Response;
import org.apache.uniffle.coordinator.web.request.ApplicationRequest;

public class ApplicationServlet extends BaseServlet<Object> {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationServlet.class);
  private final CoordinatorServer coordinator;

  public ApplicationServlet(CoordinatorServer coordinator) {
    this.coordinator = coordinator;
  }

  @Override
  protected Response<Object> handlePost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    ApplicationRequest params = parseParamsFromJson(req, ApplicationRequest.class);
    Set<String> filterApplications = new HashSet<>();
    if (params != null && CollectionUtils.isNotEmpty(params.getApplications())) {
      filterApplications = params.getApplications();
    }
    ApplicationManager applicationManager = coordinator.getApplicationManager();
    Set<Application> applicationSet = applicationManager.getApplications(filterApplications);
    return Response.success(applicationSet);
  }
}
