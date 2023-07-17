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

import io.prometheus.client.CollectorRegistry;

import org.apache.uniffle.common.exception.InvalidRequestException;

public abstract class BaseMetricResource {

  protected CollectorRegistry getCollectorRegistry(ServletContext servletContext, String type) {
    if (type == null) {
      type = "all";
    }
    CollectorRegistry registry =
        (CollectorRegistry)
            servletContext.getAttribute(CollectorRegistry.class.getCanonicalName() + "#" + type);
    if (registry == null) {
      throw new InvalidRequestException(String.format("Metric type[%s] not supported", type));
    }
    return registry;
  }
}
