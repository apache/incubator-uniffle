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
import java.io.OutputStream;
import java.util.concurrent.Callable;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.uniffle.coordinator.web.Response;

public abstract class BaseServlet extends HttpServlet {
  public static final String JSON_MIME_TYPE = "application/json";
  final ObjectMapper mapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    writeJSON(resp, handlerRequest(() -> handleGet(req, resp)));
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    writeJSON(resp, handlerRequest(() -> handlePost(req, resp)));
  }

  private <T> Response<T> handlerRequest(
      Callable<Response<T>> function) {
    Response<T> response;
    try {
      // todo: Do something for authentication
      response = function.call();
    } catch (Exception e) {
      response = Response.fail(e.getMessage());
    }
    return response;
  }

  protected <T> Response<T> handleGet(
      HttpServletRequest req,
      HttpServletResponse resp) throws ServletException, IOException {
    throw new IOException("Method not support!");
  }

  protected <T> Response<T> handlePost(
      HttpServletRequest req,
      HttpServletResponse resp) throws ServletException, IOException {
    throw new IOException("Method not support!");
  }

  protected void writeJSON(final HttpServletResponse resp, final Object obj)
      throws IOException {
    if (obj == null) {
      return;
    }
    resp.setContentType(JSON_MIME_TYPE);
    final OutputStream stream = resp.getOutputStream();
    mapper.writeValue(stream, obj);
  }

  protected <T> T parseParamsFromJson(HttpServletRequest req, Class<T> clazz) throws IOException {
    return mapper.readValue(req.getInputStream(), clazz);
  }
}
