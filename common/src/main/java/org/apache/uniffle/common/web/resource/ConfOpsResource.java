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

package org.apache.uniffle.common.web.resource;

import javax.servlet.ServletContext;

import org.apache.hbase.thirdparty.javax.ws.rs.DefaultValue;
import org.apache.hbase.thirdparty.javax.ws.rs.FormParam;
import org.apache.hbase.thirdparty.javax.ws.rs.POST;
import org.apache.hbase.thirdparty.javax.ws.rs.Path;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Context;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.config.RssConf;

@Path("/confOps")
public class ConfOpsResource {
  private static final Logger LOG = LoggerFactory.getLogger(ConfOpsResource.class);
  private static final String WARNING_MSG = "temporarily effective until restart";
  public static final String SERVLET_CONTEXT_ATTR_CONF = "_servlet_context_attr_conf_";

  @Context protected ServletContext servletContext;

  @Authorization
  @POST
  @Path("/update")
  public String update(
      @FormParam("key") String key,
      @FormParam("value") String value,
      @FormParam("delete") @DefaultValue("false") boolean delete) {
    LOG.info("Dynamic updating {} to {}, delete={}", key, value, delete);
    if (Strings.isNotEmpty(key)) {
      RssConf conf = (RssConf) servletContext.getAttribute(SERVLET_CONTEXT_ATTR_CONF);
      if (conf != null) {
        if (delete) {
          conf.remove(key);
          return String.format("Remove(%s) key: %s", WARNING_MSG, key);
        } else {
          String oldValue = conf.getString(key, null);
          conf.setString(key, value);
          return String.format("Set(%s) key: %s from %s to %s", WARNING_MSG, key, oldValue, value);
        }
      }
    }
    return "Nothing changed";
  }
}
