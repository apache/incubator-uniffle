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

import javax.annotation.Priority;

import org.apache.hbase.thirdparty.javax.ws.rs.core.Configuration;
import org.apache.hbase.thirdparty.javax.ws.rs.core.FeatureContext;
import org.apache.hbase.thirdparty.org.glassfish.jersey.internal.spi.AutoDiscoverable;

@Priority(AutoDiscoverable.DEFAULT_PRIORITY - 100)
public class JerseyAutoDiscoverable implements AutoDiscoverable {
  @Override
  public void configure(FeatureContext context) {
    Configuration config = context.getConfiguration();
    if (!config.isRegistered(JsonConverter.class)) {
      context.register(JsonConverter.class);
    }
  }
}
