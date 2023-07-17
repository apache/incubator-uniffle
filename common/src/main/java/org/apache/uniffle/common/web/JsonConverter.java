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

package org.apache.uniffle.common.web;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.nio.charset.Charset;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hbase.thirdparty.javax.ws.rs.Consumes;
import org.apache.hbase.thirdparty.javax.ws.rs.Produces;
import org.apache.hbase.thirdparty.javax.ws.rs.WebApplicationException;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MediaType;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MultivaluedMap;
import org.apache.hbase.thirdparty.javax.ws.rs.ext.MessageBodyReader;
import org.apache.hbase.thirdparty.javax.ws.rs.ext.MessageBodyWriter;
import org.apache.hbase.thirdparty.javax.ws.rs.ext.Provider;

@Provider
@Consumes({MediaType.APPLICATION_JSON, "text/json", "application/*+json"})
@Produces({MediaType.APPLICATION_JSON, "text/json", "application/*+json"})
public class JsonConverter implements MessageBodyReader<Object>, MessageBodyWriter<Object> {
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public boolean isWriteable(
      Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
    return true;
  }

  @Override
  public void writeTo(
      Object t,
      Class<?> type,
      Type genericType,
      Annotation[] annotations,
      MediaType mediaType,
      MultivaluedMap<String, Object> httpHeaders,
      OutputStream entityStream)
      throws IOException, WebApplicationException {
    objectMapper.writeValue(new OutputStreamWriter(entityStream, Charset.defaultCharset()), t);
  }

  @Override
  public boolean isReadable(
      Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
    return true;
  }

  @Override
  public Object readFrom(
      Class<Object> type,
      Type genericType,
      Annotation[] annotations,
      MediaType mediaType,
      MultivaluedMap<String, String> httpHeaders,
      InputStream entityStream)
      throws IOException, WebApplicationException {
    return objectMapper.readValue(entityStream, type);
  }

  @Override
  public long getSize(
      Object t, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
    return -1;
  }
}
