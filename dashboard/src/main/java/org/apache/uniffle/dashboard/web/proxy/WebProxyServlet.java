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

package org.apache.uniffle.dashboard.web.proxy;

import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.proxy.ProxyServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebProxyServlet extends ProxyServlet {

  private static final Logger LOG = LoggerFactory.getLogger(WebProxyServlet.class);

  private Map<String, String> coordinatorServerAddressesMap;

  public WebProxyServlet(Map<String, String> coordinatorServerAddressesMap) {
    this.coordinatorServerAddressesMap = coordinatorServerAddressesMap;
  }

  @Override
  protected String rewriteTarget(HttpServletRequest clientRequest) {
    if (!validateDestination(clientRequest.getServerName(), clientRequest.getServerPort())) {
      return null;
    }
    String targetAddress =
        coordinatorServerAddressesMap.get(clientRequest.getHeader("targetAddress"));
    if (targetAddress == null) {
      return null;
    }
    StringBuilder target = new StringBuilder();
    if (targetAddress.endsWith("/")) {
      targetAddress = targetAddress.substring(0, targetAddress.length() - 1);
    }
    target.append(targetAddress).append("/api").append(clientRequest.getPathInfo());
    String query = clientRequest.getQueryString();
    if (query != null) {
      target.append("?").append(query);
    }
    return target.toString();
  }

  @Override
  protected void onProxyRewriteFailed(
      HttpServletRequest clientRequest, HttpServletResponse clientResponse) {}

  @Override
  protected void onProxyResponseFailure(
      HttpServletRequest clientRequest,
      HttpServletResponse proxyResponse,
      Response serverResponse,
      Throwable failure) {}

  @Override
  protected String filterServerResponseHeader(
      HttpServletRequest clientRequest,
      Response serverResponse,
      String headerName,
      String headerValue) {
    return null;
  }

  @Override
  protected void addXForwardedHeaders(HttpServletRequest clientRequest, Request proxyRequest) {}
}
