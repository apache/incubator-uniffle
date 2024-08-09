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

package org.apache.uniffle.common.util.http;

import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.util.http.exception.UniffleRestException;

public class RestClientImpl implements RestClient {
  private static final Logger LOG = LoggerFactory.getLogger(RestClientImpl.class);
  private final CloseableHttpClient httpclient;
  private final String baseUrl;
  private final ObjectMapper mapper =
      new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

  public RestClientImpl(String baseUrl, CloseableHttpClient httpclient) {
    this.httpclient = httpclient;
    this.baseUrl = baseUrl;
  }

  @Override
  public void close() throws Exception {
    if (httpclient != null) {
      httpclient.close();
    }
  }

  @Override
  public String get(String path, Map<String, Object> params, String authHeader) {
    return doRequest(buildURI(path, params), authHeader, RequestBuilder.get());
  }

  @Override
  public String post(String path, Map<String, Object> params, String authHeader) {
    RequestBuilder post = RequestBuilder.post();
    String requestBody;
    try {
      requestBody = mapper.writeValueAsString(params);
      StringEntity requestEntity = new StringEntity(requestBody);
      post.setEntity(requestEntity);
    } catch (JsonProcessingException e) {
      LOG.error("params{} to json error.", params, e);
    } catch (UnsupportedEncodingException e) {
      LOG.error("params{} to StringEntity error.", params, e);
    }
    return doRequest(buildURI(path, null), authHeader, post);
  }

  private String doRequest(URI uri, String authHeader, RequestBuilder requestBuilder) {
    String response;
    try {
      if (requestBuilder.getFirstHeader(HttpHeaders.CONTENT_TYPE) == null) {
        requestBuilder.setHeader(
            HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
      }
      if (StringUtils.isNotBlank(authHeader)) {
        requestBuilder.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
      }
      HttpUriRequest httpRequest = requestBuilder.setUri(uri).build();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Executing {} request: {}", httpRequest.getMethod(), uri);
      }

      ResponseHandler<String> responseHandler =
          resp -> {
            int status = resp.getStatusLine().getStatusCode();
            HttpEntity entity = resp.getEntity();
            String entityStr = entity != null ? EntityUtils.toString(entity) : null;
            if (status >= 200 && status < 300) {
              return entityStr;
            } else {
              throw new HttpResponseException(status, entityStr);
            }
          };

      response = httpclient.execute(httpRequest, responseHandler);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Response: {}", response);
      }
    } catch (ConnectException | ConnectTimeoutException | NoHttpResponseException e) {
      throw new UniffleRestException("Api request failed for " + uri.toString(), e);
    } catch (UniffleRestException rethrow) {
      throw rethrow;
    } catch (Exception e) {
      LOG.error("Error: ", e);
      throw new UniffleRestException("Api request failed for " + uri.toString(), e);
    }

    return response;
  }

  private URI buildURI(String path, Map<String, Object> params) {
    URI uri;
    try {
      String url;
      if (StringUtils.isBlank(this.baseUrl)) {
        url = path;
      } else {
        url = StringUtils.isNotBlank(path) ? this.baseUrl + "/" + path : this.baseUrl;
      }

      URIBuilder builder = new URIBuilder(url);

      if (params != null && !params.isEmpty()) {
        for (Map.Entry<String, Object> entry : params.entrySet()) {
          if (entry.getValue() != null) {
            builder.addParameter(entry.getKey(), entry.getValue().toString());
          }
        }
      }
      uri = builder.build();
    } catch (URISyntaxException e) {
      throw new UniffleRestException("invalid URI.", e);
    }
    return uri;
  }
}
