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

package org.apache.uniffle.storage.handler.impl;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.storage.handler.ClientReadHandlerMetric;
import org.apache.uniffle.storage.handler.api.ClientReadHandler;

/**
 * Composed read handler for all storage types and one replicas.
 * The storage types reading order is as follows: HOT -> WARM -> COLD -> FROZEN
 * @see <a href="https://github.com/apache/incubator-uniffle/pull/276">PR-276</a>
 */
public class ComposedClientReadHandler extends AbstractClientReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ComposedClientReadHandler.class);

  private enum Tier {
    HOT, WARM, COLD, FROZEN;

    static final Tier[] VALUES = Tier.values();

    Tier next() {
      return VALUES[this.ordinal() + 1];
    }
  }

  private final ShuffleServerInfo serverInfo;
  private final Map<Tier, Supplier<ClientReadHandler>> supplierMap = new EnumMap<>(Tier.class);
  private final Map<Tier, ClientReadHandler> handlerMap = new EnumMap<>(Tier.class);
  private final Map<Tier, ClientReadHandlerMetric> metricsMap = new EnumMap<>(Tier.class);
  private Tier currentTier = Tier.VALUES[0];
  private final int numTiers;

  public ComposedClientReadHandler(ShuffleServerInfo serverInfo, ClientReadHandler... handlers) {
    this.serverInfo = serverInfo;
    numTiers = Math.min(Tier.VALUES.length, handlers.length);
    for (int i = 0; i < numTiers; i++) {
      handlerMap.put(Tier.VALUES[i], handlers[i]);
    }
    for (Tier tier : Tier.VALUES) {
      metricsMap.put(tier, new ClientReadHandlerMetric());
    }
  }

  public ComposedClientReadHandler(ShuffleServerInfo serverInfo, List<Supplier<ClientReadHandler>> callables) {
    this.serverInfo = serverInfo;
    numTiers = Math.min(Tier.VALUES.length, callables.size());
    for (int i = 0; i < numTiers; i++) {
      supplierMap.put(Tier.VALUES[i], callables.get(i));
    }
    for (Tier tier : Tier.VALUES) {
      metricsMap.put(tier, new ClientReadHandlerMetric());
    }
  }

  @Override
  public ShuffleDataResult readShuffleData() {
    ShuffleDataResult shuffleDataResult = null;
    try {
      ClientReadHandler handler = handlerMap.computeIfAbsent(currentTier,
          key -> supplierMap.getOrDefault(key, () -> null).get());
      if (handler == null) {
        return null;
      }
      shuffleDataResult = handler.readShuffleData();
    } catch (Exception e) {
      throw new RssException("Failed to read shuffle data from " + currentTier.name() + " handler", e);
    }
    // when is no data for current handler, and the upmostLevel is not reached,
    // then try next one if there has
    if (shuffleDataResult == null || shuffleDataResult.isEmpty()) {
      if (currentTier.ordinal() + 1 < numTiers) {
        currentTier = currentTier.next();
      } else {
        return null;
      }
      return readShuffleData();
    }

    return shuffleDataResult;
  }

  @Override
  public void close() {
    for (ClientReadHandler handler : handlerMap.values()) {
      handler.close();
    }
  }

  @Override
  public void updateConsumedBlockInfo(BufferSegment bs, boolean isSkippedMetrics) {
    if (bs == null) {
      return;
    }
    super.updateConsumedBlockInfo(bs, isSkippedMetrics);
    updateBlockMetric(metricsMap.get(currentTier), bs, isSkippedMetrics);
  }

  @Override
  public void logConsumedBlockInfo() {
    LOG.info(getReadBlockNumInfo());
    LOG.info(getReadLengthInfo());
    LOG.info(getReadUncompressLengthInfo());
  }

  @VisibleForTesting
  public String getReadBlockNumInfo() {
    return getMetricsInfo("blocks", ClientReadHandlerMetric::getReadBlockNum,
        ClientReadHandlerMetric::getSkippedReadBlockNum);
  }

  @VisibleForTesting
  public String getReadLengthInfo() {
    return getMetricsInfo("bytes", ClientReadHandlerMetric::getReadLength,
        ClientReadHandlerMetric::getSkippedReadLength);
  }

  @VisibleForTesting
  public String getReadUncompressLengthInfo() {
    return getMetricsInfo("uncompressed bytes", ClientReadHandlerMetric::getReadUncompressLength,
        ClientReadHandlerMetric::getSkippedReadUncompressLength);
  }

  private String getMetricsInfo(String name, Function<ClientReadHandlerMetric, Long> consumed,
      Function<ClientReadHandlerMetric, Long> skipped) {
    return "Client read " + consumed.apply(readHandlerMetric)
        + " " + name + " from [" + serverInfo + "], Consumed["
        + " hot:" + consumed.apply(metricsMap.get(Tier.HOT))
        + " warm:" + consumed.apply(metricsMap.get(Tier.WARM))
        + " cold:" + consumed.apply(metricsMap.get(Tier.COLD))
        + " frozen:" + consumed.apply(metricsMap.get(Tier.FROZEN)) + " ], Skipped["
        + " hot:" + skipped.apply(metricsMap.get(Tier.HOT))
        + " warm:" + skipped.apply(metricsMap.get(Tier.WARM))
        + " cold:" + skipped.apply(metricsMap.get(Tier.COLD))
        + " frozen:" + skipped.apply(metricsMap.get(Tier.FROZEN)) + " ]";
  }

}
