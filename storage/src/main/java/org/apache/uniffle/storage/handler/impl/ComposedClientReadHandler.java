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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
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

  private final ShuffleServerInfo serverInfo;
  private ClientReadHandler hotDataReadHandler;
  private ClientReadHandler warmDataReadHandler;
  private ClientReadHandler coldDataReadHandler;
  private ClientReadHandler frozenDataReadHandler;
  private static final int HOT = 1;
  private static final int WARM = 2;
  private static final int COLD = 3;
  private static final int FROZEN = 4;
  private int currentHandler = HOT;
  private final int topLevelOfHandler;

  private ClientReadHandlerMetric hostHandlerMetric = new ClientReadHandlerMetric();
  private ClientReadHandlerMetric warmHandlerMetric = new ClientReadHandlerMetric();
  private ClientReadHandlerMetric coldHandlerMetric = new ClientReadHandlerMetric();
  private ClientReadHandlerMetric frozenHandlerMetric = new ClientReadHandlerMetric();

  public ComposedClientReadHandler(ShuffleServerInfo serverInfo, ClientReadHandler... handlers) {
    this(serverInfo, Lists.newArrayList(handlers));
  }

  public ComposedClientReadHandler(ShuffleServerInfo serverInfo, List<ClientReadHandler> handlers) {
    this.serverInfo = serverInfo;
    topLevelOfHandler = handlers.size();
    if (topLevelOfHandler > 0) {
      this.hotDataReadHandler = handlers.get(0);
    }
    if (topLevelOfHandler > 1) {
      this.warmDataReadHandler = handlers.get(1);
    }
    if (topLevelOfHandler > 2) {
      this.coldDataReadHandler = handlers.get(2);
    }
    if (topLevelOfHandler > 3) {
      this.frozenDataReadHandler = handlers.get(3);
    }
  }

  @Override
  public ShuffleDataResult readShuffleData() {
    ShuffleDataResult shuffleDataResult = null;
    try {
      switch (currentHandler) {
        case HOT:
          shuffleDataResult = hotDataReadHandler.readShuffleData();
          break;
        case WARM:
          shuffleDataResult = warmDataReadHandler.readShuffleData();
          break;
        case COLD:
          shuffleDataResult = coldDataReadHandler.readShuffleData();
          break;
        case FROZEN:
          shuffleDataResult = frozenDataReadHandler.readShuffleData();
          break;
        default:
          return null;
      }
    } catch (Exception e) {
      throw new RssException("Failed to read shuffle data from " + getCurrentHandlerName() + " handler", e);
    }
    // when is no data for current handler, and the upmostLevel is not reached,
    // then try next one if there has
    if (shuffleDataResult == null || shuffleDataResult.isEmpty()) {
      if (currentHandler < topLevelOfHandler) {
        currentHandler++;
      } else {
        return null;
      }
      return readShuffleData();
    }

    return shuffleDataResult;
  }

  private String getCurrentHandlerName() {
    String name = "UNKNOWN";
    switch (currentHandler) {
      case HOT:
        name = "HOT";
        break;
      case WARM:
        name = "WARM";
        break;
      case COLD:
        name = "COLD";
        break;
      case FROZEN:
        name = "FROZEN";
        break;
      default:
        break;
    }
    return name;
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
    LOG.info(getReadBlokNumInfo());
    LOG.info(getReadLengthInfo());
    LOG.info(getReadUncompressLengthInfo());
  }

  @VisibleForTesting
  public String getReadBlokNumInfo() {
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
