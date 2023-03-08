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

package org.apache.spark.shuffle.reader;

import java.io.IOException;

import org.apache.spark.shuffle.FetchFailedException;
import org.apache.spark.shuffle.RssSparkShuffleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Product2;
import scala.collection.AbstractIterator;
import scala.collection.Iterator;

import org.apache.uniffle.client.api.ShuffleManagerClient;
import org.apache.uniffle.client.request.RssReportShuffleFetchFailureRequest;
import org.apache.uniffle.client.response.RssReportShuffleFetchFailureResponse;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssFetchFailedException;

public class ShuffleFetchFailedWrapper {
  private static final Logger LOG = LoggerFactory.getLogger(ShuffleFetchFailedWrapper.class);
  private RssConf conf;
  private String appId;
  private int shuffleId;
  private int partitionId;
  private int stageAttemptId;
  private String reportServerHost;
  private int reportServerPort;

  private ShuffleFetchFailedWrapper() {

  }

  static ShuffleFetchFailedWrapper newWrapper(RssConf conf) {
    ShuffleFetchFailedWrapper wrapper = new ShuffleFetchFailedWrapper();
    wrapper.conf = conf;
    return wrapper;
  }

  ShuffleFetchFailedWrapper appId(String appId) {
    this.appId = appId;
    return this;
  }

  ShuffleFetchFailedWrapper shuffleId(int shuffleId) {
    this.shuffleId = shuffleId;
    return this;
  }

  ShuffleFetchFailedWrapper partitionId(int partitionId) {
    this.partitionId = partitionId;
    return this;
  }

  ShuffleFetchFailedWrapper stageAttemptId(int stageAttemptId) {
    this.stageAttemptId = stageAttemptId;
    return this;
  }

  ShuffleFetchFailedWrapper reportServerHost(String host) {
    this.reportServerHost = host;
    return this;
  }

  ShuffleFetchFailedWrapper port(int port) {
    this.reportServerPort = port;
    return this;
  }


  public <K, C> Iterator<Product2<K, C>> wrap(Iterator<Product2<K, C>> iter) {
    return new IteratorImpl<>(iter, this);
  }

  private static class IteratorImpl<K, C> extends AbstractIterator<Product2<K, C>> {
    private Iterator<Product2<K, C>> iter;
    private ShuffleFetchFailedWrapper wrapper;

    IteratorImpl(Iterator<Product2<K, C>> iter, ShuffleFetchFailedWrapper wrapper) {
      this.iter = iter;
      this.wrapper = wrapper;
    }

    private RuntimeException generateFetchFailedIfNecessary(RssFetchFailedException e) {
      String driver = wrapper.reportServerHost;
      int port = wrapper.reportServerPort;
      // todo: reuse this manager client if this is a bottleneck.
      try (ShuffleManagerClient client = RssSparkShuffleUtils.createShuffleManagerClient(driver, port)) {
        RssReportShuffleFetchFailureRequest req = new RssReportShuffleFetchFailureRequest(
            wrapper.appId, wrapper.shuffleId, wrapper.stageAttemptId, wrapper.partitionId, e.getMessage(), null);
        RssReportShuffleFetchFailureResponse response =  client.reportShuffleFetchFailure(req);
        if (response.getRecomputeStage()) {
          // since we are going to roll out the whole stage, mapIndex shouldn't matter, hence -1 is provided.
          FetchFailedException ffe = RssSparkShuffleUtils.createFetchFailedException(wrapper.shuffleId, -1,
              wrapper.partitionId, e);
          return new RuntimeException(ffe);
        }
      } catch (IOException ioe) {
        LOG.info("Error closing shuffle manager client with error:", ioe);
      }
      return e;
    }

    @Override
    public boolean hasNext() {
      try {
        return this.iter.hasNext();
      } catch (RssFetchFailedException e) {
        throw generateFetchFailedIfNecessary(e);
      }
    }

    @Override
    public Product2<K, C> next() {
      try {
        return this.iter.next();
      } catch (RssFetchFailedException e) {
        throw generateFetchFailedIfNecessary(e);
      }
    }
  }
}
