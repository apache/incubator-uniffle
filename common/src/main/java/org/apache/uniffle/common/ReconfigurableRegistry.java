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

package org.apache.uniffle.common;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.config.RssConf;

public class ReconfigurableRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(ReconfigurableRegistry.class);

  private static final List<ReconfigureListener> LISTENER_LIST = new LinkedList<>();

  /**
   * Add a listener.
   *
   * @param listener the given property listener
   */
  public static synchronized void register(ReconfigureListener listener) {
    LISTENER_LIST.add(listener);
  }

  /**
   * remove the listener related to the given property.
   *
   * @param listener the listener
   * @return true if the instance is removed
   */
  public static synchronized boolean unregister(ReconfigureListener listener) {
    return LISTENER_LIST.remove(listener);
  }

  // prevent instantiation
  private ReconfigurableRegistry() {}

  /**
   * When the property was reconfigured, this function will be invoked. This property listeners will
   * be notified.
   *
   * @param conf the rss conf
   * @param changedProperties the changed properties
   */
  public static synchronized void update(RssConf conf, Map<String, Object> changedProperties) {
    Throwable lastException = null;
    for (ReconfigureListener listener : LISTENER_LIST) {
      try {
        listener.update(conf, changedProperties);
      } catch (Throwable e) {
        LOG.warn("Exception while update config for {}", changedProperties, e);
        lastException = e;
      }
    }
    if (lastException != null) {
      throw new IllegalStateException(
          "last exception while update config " + changedProperties, lastException);
    }
  }

  public interface ReconfigureListener {
    /**
     * When the property changed, this function will be invoked.
     *
     * @param conf the rss conf
     * @param changedProperties the changed properties
     */
    void update(RssConf conf, Map<String, Object> changedProperties);
  }
}
