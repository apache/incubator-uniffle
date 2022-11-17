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

import java.util.concurrent.Callable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.storage.handler.api.ClientReadHandler;

/**
 * Composed read handler for all storage types and all replicas.
 * 1.The storage types reading order is as follows: HOT -> WARM -> COLD -> FROZEN
 * 2.If blocks read inconsistent in the client of upper level. {@link ComposedClientReadHandler#nextRound}
 *   will be invoked and start read from HOT storage.
 * 3.Each storage handler determines how to read their replicas.
 * @see <a href="https://github.com/apache/incubator-uniffle/pull/276">PR-276</a>
 */
public class ComposedClientReadHandler implements ClientReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ComposedClientReadHandler.class);

  private ClientReadHandler hotDataReadHandler;
  private ClientReadHandler warmDataReadHandler;
  private ClientReadHandler coldDataReadHandler;
  private ClientReadHandler frozenDataReadHandler;
  private Callable<ClientReadHandler> hotHandlerCreator;
  private Callable<ClientReadHandler> warmHandlerCreator;
  private Callable<ClientReadHandler> coldHandlerCreator;
  private Callable<ClientReadHandler> frozenHandlerCreator;
  private static final int HOT = 1;
  private static final int WARM = 2;
  private static final int COLD = 3;
  private static final int FROZEN = 4;
  private int currentHandler = HOT;
  private final int topLevelOfHandler;

  private long hotReadBlockNum = 0L;
  private long warmReadBlockNum = 0L;
  private long coldReadBlockNum = 0L;
  private long frozenReadBlockNum = 0L;

  private long hotReadLength = 0L;
  private long warmReadLength = 0L;
  private long coldReadLength = 0L;
  private long frozenReadLength = 0L;

  private long hotReadUncompressLength = 0L;
  private long warmReadUncompressLength = 0L;
  private long coldReadUncompressLength = 0L;
  private long frozenReadUncompressLength = 0L;

  public ComposedClientReadHandler(ClientReadHandler... handlers) {
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

  public ComposedClientReadHandler(Callable<ClientReadHandler>... creators) {
    topLevelOfHandler = creators.length;
    if (topLevelOfHandler > 0) {
      this.hotHandlerCreator = creators[0];
    }
    if (topLevelOfHandler > 1) {
      this.warmHandlerCreator = creators[1];
    }
    if (topLevelOfHandler > 2) {
      this.coldHandlerCreator = creators[2];
    }
    if (topLevelOfHandler > 3) {
      this.frozenHandlerCreator = creators[3];
    }
  }

  @Override
  public ShuffleDataResult readShuffleData() {
    ShuffleDataResult shuffleDataResult = null;
    try {
      switch (currentHandler) {
        case HOT:
          if (hotDataReadHandler == null) {
            hotDataReadHandler = createReadHandlerIfNotExist(hotHandlerCreator);
          }
          shuffleDataResult = hotDataReadHandler.readShuffleData();
          break;
        case WARM:
          if (warmDataReadHandler == null) {
            warmDataReadHandler = createReadHandlerIfNotExist(warmHandlerCreator);
          }
          shuffleDataResult = warmDataReadHandler.readShuffleData();
          break;
        case COLD:
          if (coldDataReadHandler == null) {
            coldDataReadHandler = createReadHandlerIfNotExist(coldHandlerCreator);
          }
          shuffleDataResult = coldDataReadHandler.readShuffleData();
          break;
        case FROZEN:
          if (frozenDataReadHandler == null) {
            frozenDataReadHandler = createReadHandlerIfNotExist(frozenHandlerCreator);
          }
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

  private ClientReadHandler createReadHandlerIfNotExist(Callable<ClientReadHandler> creator) throws Exception {
    if (creator == null) {
      throw new IllegalStateException("creator " + getCurrentHandlerName() + " handler doesn't exist");
    }
    return creator.call();
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
  public void updateConsumedBlockInfo(BufferSegment bs) {
    if (bs == null) {
      return;
    }
    switch (currentHandler) {
      case HOT:
        hotReadBlockNum++;
        hotReadLength += bs.getLength();
        hotReadUncompressLength += bs.getUncompressLength();
        break;
      case WARM:
        warmReadBlockNum++;
        warmReadLength += bs.getLength();
        warmReadUncompressLength += bs.getUncompressLength();
        break;
      case COLD:
        coldReadBlockNum++;
        coldReadLength += bs.getLength();
        coldReadUncompressLength += bs.getUncompressLength();
        break;
      case FROZEN:
        frozenReadBlockNum++;
        frozenReadLength += bs.getLength();
        frozenReadUncompressLength += bs.getUncompressLength();
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

  /**
   * If blocks read inconsistent, we need try again.
   * And the low-level handlers may need do something before try.
   */
  @Override
  public void nextRound() {
    currentHandler = HOT;
    if (hotDataReadHandler != null) {
      hotDataReadHandler.nextRound();
    }
    if (warmDataReadHandler != null) {
      warmDataReadHandler.nextRound();
    }
    if (coldDataReadHandler != null) {
      coldDataReadHandler.nextRound();
    }
    if (frozenDataReadHandler != null) {
      frozenDataReadHandler.nextRound();
    }
  }

  @Override
  public boolean finished() {
    if (hotDataReadHandler != null && !hotDataReadHandler.finished()) {
      return false;
    }
    if (warmDataReadHandler != null && !warmDataReadHandler.finished()) {
      return false;
    }
    if (coldDataReadHandler != null && !coldDataReadHandler.finished()) {
      return false;
    }
    if (frozenDataReadHandler != null && !frozenDataReadHandler.finished()) {
      return false;
    }
    return true;
  }

  @VisibleForTesting
  public String getReadBlokNumInfo() {
    long totalBlockNum = hotReadBlockNum + warmReadBlockNum
        + coldReadBlockNum + frozenReadBlockNum;
    return "Client read " + totalBlockNum + " blocks ["
        + " hot:" + hotReadBlockNum + " warm:" + warmReadBlockNum
        + " cold:" + coldReadBlockNum + " frozen:" + frozenReadBlockNum + " ]";
  }

  @VisibleForTesting
  public String getReadLengthInfo() {
    long totalReadLength = hotReadLength + warmReadLength
        + coldReadLength + frozenReadLength;
    return "Client read " + totalReadLength + " bytes ["
        + " hot:" + hotReadLength + " warm:" + warmReadLength
        + " cold:" + coldReadLength + " frozen:" + frozenReadLength + " ]";
  }

  @VisibleForTesting
  public String getReadUncompressLengthInfo() {
    long totalReadUncompressLength = hotReadUncompressLength + warmReadUncompressLength
        + coldReadUncompressLength + frozenReadUncompressLength;
    return "Client read " + totalReadUncompressLength + " uncompressed bytes ["
        + " hot:" + hotReadUncompressLength
        + " warm:" + warmReadUncompressLength
        + " cold:" + coldReadUncompressLength
        + " frozen:" + frozenReadUncompressLength + " ]";
  }

}
