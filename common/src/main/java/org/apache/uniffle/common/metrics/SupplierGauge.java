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

package org.apache.uniffle.common.metrics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;

class SupplierGauge extends Collector implements Collector.Describable {
  private String name;
  private String help;
  private Supplier<Double> supplier;
  private List<String> labelNames;
  private List<String> labelValues;

  SupplierGauge(
      String name,
      String help,
      Supplier<Double> supplier,
      String[] labelNames,
      String[] labelValues) {
    this.name = name;
    this.help = help;
    this.supplier = supplier;
    this.labelNames = Arrays.asList(labelNames);
    this.labelValues = Arrays.asList(labelValues);
  }

  @Override
  public List<MetricFamilySamples> collect() {
    List<MetricFamilySamples.Sample> samples = new ArrayList<>();
    samples.add(
        new MetricFamilySamples.Sample(
            this.name, this.labelNames, this.labelValues, this.supplier.get()));
    MetricFamilySamples mfs = new MetricFamilySamples(this.name, Type.GAUGE, this.help, samples);
    List<MetricFamilySamples> mfsList = new ArrayList<MetricFamilySamples>(1);
    mfsList.add(mfs);
    return mfsList;
  }

  @Override
  public List<MetricFamilySamples> describe() {
    return Collections.<MetricFamilySamples>singletonList(
        new GaugeMetricFamily(this.name, this.help, this.labelNames));
  }
}
