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

package org.apache.uniffle.coordinator.web.servlet.admin;

import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.coordinator.CoordinatorServer;
import org.apache.uniffle.coordinator.access.checker.AccessChecker;
import org.apache.uniffle.coordinator.web.Response;
import org.apache.uniffle.coordinator.web.servlet.BaseServlet;

public class RefreshCheckerServlet extends BaseServlet {

  private static final Logger LOG = LoggerFactory.getLogger(RefreshCheckerServlet.class);
  private final CoordinatorServer coordinator;

  public RefreshCheckerServlet(CoordinatorServer coordinator) {
    this.coordinator = coordinator;
  }

  @Override
  protected Response handleGet(HttpServletRequest req, HttpServletResponse resp) {
    List<AccessChecker> accessCheckers = coordinator.getAccessManager().getAccessCheckers();
    LOG.info(
        "The access checker {} has been refreshed, you can add the checker via rss.coordinator.access.checkers.",
        accessCheckers);
    accessCheckers.forEach(AccessChecker::refreshAccessChecker);
    return Response.success(null);
  }
}
