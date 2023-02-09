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

import java.util.List;
import java.util.concurrent.Callable;

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

    public Tier next() {
      return values()[this.ordinal() + 1];
    }
  }

  private final ShuffleServerInfo serverInfo;
  private Callable<ClientReadHandler> hotHandlerCreator;
  private Callable<ClientReadHandler> warmHandlerCreator;
  private Callable<ClientReadHandler> coldHandlerCreator;
  private Callable<ClientReadHandler> frozenHandlerCreator;
  private ClientReadHandler hotDataReadHandler;
  private ClientReadHandler warmDataReadHandler;
  private ClientReadHandler coldDataReadHandler;
  private ClientReadHandler frozenDataReadHandler;
  private Tier currentHandler = Tier.HOT;
  private final int topLevelOfHandler;

  private ClientReadHandlerMetric hostHandlerMetric = new ClientReadHandlerMetric();
  private ClientReadHandlerMetric warmHandlerMetric = new ClientReadHandlerMetric();
  private ClientReadHandlerMetric coldHandlerMetric = new ClientReadHandlerMetric();
  private ClientReadHandlerMetric frozenHandlerMetric = new ClientReadHandlerMetric();

  public ComposedClientReadHandler(ShuffleServerInfo serverInfo, ClientReadHandler... handlers) {
    this.serverInfo = serverInfo;
    topLevelOfHandler = handlers.length;
    if (topLevelOfHandler > 0) {
      this.hotDataReadHandler = handlers[0];
    }
    if (topLevelOfHandler > 1) {
      this.warmDataReadHandler = handlers[1];
    }
    if (topLevelOfHandler > 2) {
      this.coldDataReadHandler = handlers[2];
    }
    if (topLevelOfHandler > 3) {
      this.frozenDataReadHandler = handlers[3];
    }
  }

  public ComposedClientReadHandler(ShuffleServerInfo serverInfo, List<Callable<ClientReadHandler>> callables) {
    this.serverInfo = serverInfo;
    topLevelOfHandler = callables.size();
    if (topLevelOfHandler > 0) {
      this.hotHandlerCreator = callables.get(0);
    }
    if (topLevelOfHandler > 1) {
      this.warmHandlerCreator = callables.get(1);
    }
    if (topLevelOfHandler > 2) {
      this.coldHandlerCreator = callables.get(2);
    }
    if (topLevelOfHandler > 3) {
      this.frozenHandlerCreator = callables.get(3);
    }
  }

  @Override
  public ShuffleDataResult readShuffleData() {
    ShuffleDataResult shuffleDataResult = null;
    try {
      switch (currentHandler) {
        case HOT:
          if (hotDataReadHandler == null) {
            hotDataReadHandler = hotHandlerCreator.call();
          }
          shuffleDataResult = hotDataReadHandler.readShuffleData();
          break;
        case WARM:
          if (warmDataReadHandler == null) {
            warmDataReadHandler = warmHandlerCreator.call();
          }
          shuffleDataResult = warmDataReadHandler.readShuffleData();
          break;
        case COLD:
          if (coldDataReadHandler == null) {
            coldDataReadHandler = coldHandlerCreator.call();
          }
          shuffleDataResult = coldDataReadHandler.readShuffleData();
          break;
        case FROZEN:
          if (frozenDataReadHandler == null) {
            frozenDataReadHandler = frozenHandlerCreator.call();
          }
          shuffleDataResult = frozenDataReadHandler.readShuffleData();
          break;
        default:
          return null;
      }
    } catch (Exception e) {
      throw new RssException("Failed to read shuffle data from " + currentHandler.name() + " handler", e);
    }
    // when is no data for current handler, and the upmostLevel is not reached,
    // then try next one if there has
    if (shuffleDataResult == null || shuffleDataResult.isEmpty()) {
      if (currentHandler.ordinal() + 1 < topLevelOfHandler) {
        currentHandler = currentHandler.next();
      } else {
        return null;
      }
      return readShuffleData();
    }

    return shuffleDataResult;
  }

  @Override
  public void close() {
    if (hotDataReadHandler != null) {
      hotDataReadHandler.close();
    }

    if (warmDataReadHandler != null) {
      warmDataReadHandler.close();
    }

    if (coldDataReadHandler != null) {
      coldDataReadHandler.close();
    }

    if (frozenDataReadHandler != null) {
      frozenDataReadHandler.close();
    }
  }

  @Override
  public void updateConsumedBlockInfo(BufferSegment bs, boolean isSkippedMetrics) {
    if (bs == null) {
      return;
    }
    super.updateConsumedBlockInfo(bs, isSkippedMetrics);
    switch (currentHandler) {
      case HOT:
        updateBlockMetric(hostHandlerMetric, bs, isSkippedMetrics);
        break;
      case WARM:
        updateBlockMetric(warmHandlerMetric, bs, isSkippedMetrics);
        break;
      case COLD:
        updateBlockMetric(coldHandlerMetric, bs, isSkippedMetrics);
        break;
      case FROZEN:
        updateBlockMetric(frozenHandlerMetric, bs, isSkippedMetrics);
        break;
      default:
        break;
    }
  }

  @Override
  public void logConsumedBlockInfo() {
    LOG.info(getReadBlockNumInfo());
    LOG.info(getReadLengthInfo());
    LOG.info(getReadUncompressLengthInfo());
  }

  @VisibleForTesting
  public String getReadBlockNumInfo() {
    return "Client read " + readHandlerMetric.getReadBlockNum()
        + " blocks from [" + serverInfo + "], Consumed["
        + " hot:" + hostHandlerMetric.getReadBlockNum()
        + " warm:" + warmHandlerMetric.getReadBlockNum()
        + " cold:" + coldHandlerMetric.getReadBlockNum()
        + " frozen:" + frozenHandlerMetric.getReadBlockNum()
        + " ], Skipped[" + " hot:" + hostHandlerMetric.getSkippedReadBlockNum()
        + " warm:" + warmHandlerMetric.getSkippedReadBlockNum()
        + " cold:" + coldHandlerMetric.getSkippedReadBlockNum()
        + " frozen:" + frozenHandlerMetric.getSkippedReadBlockNum() + " ]";
  }

  @VisibleForTesting
  public String getReadLengthInfo() {
    return "Client read " + readHandlerMetric.getReadLength()
        + " bytes from [" + serverInfo + "], Consumed["
        + " hot:" + hostHandlerMetric.getReadLength()
        + " warm:" + warmHandlerMetric.getReadLength()
        + " cold:" + coldHandlerMetric.getReadLength()
        + " frozen:" + frozenHandlerMetric.getReadLength() + " ], Skipped["
        + " hot:" + hostHandlerMetric.getSkippedReadLength()
        + " warm:" + warmHandlerMetric.getSkippedReadLength()
        + " cold:" + coldHandlerMetric.getSkippedReadLength()
        + " frozen:" + frozenHandlerMetric.getSkippedReadLength() + " ]";
  }

  @VisibleForTesting
  public String getReadUncompressLengthInfo() {
    return "Client read " + readHandlerMetric.getReadUncompressLength()
        + " uncompressed bytes from [" + serverInfo + "], Consumed["
        + " hot:" + hostHandlerMetric.getReadUncompressLength()
        + " warm:" + warmHandlerMetric.getReadUncompressLength()
        + " cold:" + coldHandlerMetric.getReadUncompressLength()
        + " frozen:" + frozenHandlerMetric.getReadUncompressLength() + " ], Skipped["
        + " hot:" + hostHandlerMetric.getSkippedReadUncompressLength()
        + " warm:" + warmHandlerMetric.getSkippedReadUncompressLength()
        + " cold:" + coldHandlerMetric.getSkippedReadUncompressLength()
        + " frozen:" + frozenHandlerMetric.getSkippedReadUncompressLength() + " ]";
  }

}
