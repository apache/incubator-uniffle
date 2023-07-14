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

import java.util.Enumeration;
import java.util.Set;
import java.util.function.Function;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;

public class CoalescedCollectorRegistry extends CollectorRegistry {
  private final CollectorRegistry[] registries;

  public CoalescedCollectorRegistry(CollectorRegistry... registries) {
    this.registries = registries;
  }

  @Override
  public Enumeration<Collector.MetricFamilySamples> metricFamilySamples() {
    return new CoalescedEnumeration((index) -> registries[index].metricFamilySamples());
  }

  @Override
  public Enumeration<Collector.MetricFamilySamples> filteredMetricFamilySamples(
      Set<String> includedNames) {
    return new CoalescedEnumeration(
        (index) -> registries[index].filteredMetricFamilySamples(includedNames));
  }

  @Override
  public Double getSampleValue(String name) {
    return this.getSampleValue(name, new String[0], new String[0]);
  }

  @Override
  public Double getSampleValue(String name, String[] labelNames, String[] labelValues) {
    Double ret = null;
    for (CollectorRegistry collectorRegistry : registries) {
      ret = collectorRegistry.getSampleValue(name, labelNames, labelValues);
      if (ret != null) {
        return ret;
      }
    }
    return ret;
  }

  private class CoalescedEnumeration implements Enumeration<Collector.MetricFamilySamples> {
    private final Function<Integer, Enumeration<Collector.MetricFamilySamples>> function;
    Enumeration<Collector.MetricFamilySamples> currentEnumeration;
    int index = 0;

    CoalescedEnumeration(Function<Integer, Enumeration<Collector.MetricFamilySamples>> function) {
      this.function = function;
    }

    @Override
    public boolean hasMoreElements() {
      if (currentEnumeration == null || !currentEnumeration.hasMoreElements()) {
        if (index >= registries.length) {
          return false;
        }
        currentEnumeration = function.apply(index++);
      }
      return currentEnumeration.hasMoreElements();
    }

    @Override
    public Collector.MetricFamilySamples nextElement() {
      if (!currentEnumeration.hasMoreElements()) {
        return null;
      }
      return currentEnumeration.nextElement();
    }
  }
}
