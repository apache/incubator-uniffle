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
import java.util.ArrayList;
import java.util.Objects;

import scala.Product2;
import scala.collection.AbstractIterator;
import scala.collection.Iterator;

import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;
import org.apache.spark.shuffle.FetchFailedException;
import org.apache.spark.shuffle.RssSparkShuffleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleManagerClient;
import org.apache.uniffle.client.factory.ShuffleManagerClientFactory;
import org.apache.uniffle.client.request.RssReportShuffleFetchFailureRequest;
import org.apache.uniffle.client.response.RssReportShuffleFetchFailureResponse;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.exception.RssFetchFailedException;

public class RssFetchFailedIterator<K, C> extends AbstractIterator<Product2<K, C>> {
  private static final Logger LOG = LoggerFactory.getLogger(RssFetchFailedIterator.class);
  final Iterator<Product2<K, C>> iter;
  final Builder builder;

  private RssFetchFailedIterator(Builder builder, Iterator<Product2<K, C>> iterator) {
    this.builder = builder;
    this.iter = iterator;
  }

  public static class Builder {
    private String appId;
    private int shuffleId;
    private int partitionId;
    private int stageAttemptId;
    private String reportServerHost;
    private int reportServerPort;

    private Builder() {}

    Builder appId(String appId) {
      this.appId = appId;
      return this;
    }

    Builder shuffleId(int shuffleId) {
      this.shuffleId = shuffleId;
      return this;
    }

    Builder partitionId(int partitionId) {
      this.partitionId = partitionId;
      return this;
    }

    Builder stageAttemptId(int stageAttemptId) {
      this.stageAttemptId = stageAttemptId;
      return this;
    }

    Builder reportServerHost(String host) {
      this.reportServerHost = host;
      return this;
    }

    Builder port(int port) {
      this.reportServerPort = port;
      return this;
    }

    <K, C> RssFetchFailedIterator<K, C> build(Iterator<Product2<K, C>> iter) {
      Objects.requireNonNull(this.appId);
      Objects.requireNonNull(this.reportServerHost);
      return new RssFetchFailedIterator<>(this, iter);
    }
  }

  static Builder newBuilder() {
    return new Builder();
  }

  private static ShuffleManagerClient createShuffleManagerClient(String host, int port)
      throws IOException {
    ClientType grpc = ClientType.GRPC;
    // host is passed from spark.driver.bindAddress, which would be set when SparkContext is
    // constructed.
    return ShuffleManagerClientFactory.getInstance().createShuffleManagerClient(grpc, host, port);
  }

  private RssException generateFetchFailedIfNecessary(RssFetchFailedException e) {
    String driver = builder.reportServerHost;
    int port = builder.reportServerPort;
    // todo: reuse this manager client if this is a bottleneck.
    try (ShuffleManagerClient client = createShuffleManagerClient(driver, port)) {
      TaskContext taskContext = TaskContext$.MODULE$.get();
      RssReportShuffleFetchFailureRequest req =
          new RssReportShuffleFetchFailureRequest(
              builder.appId,
              builder.shuffleId,
              builder.stageAttemptId,
              builder.partitionId,
              e.getMessage(),
              new ArrayList<>(e.getFetchFailureServerIds()),
              taskContext.stageId(),
              taskContext.taskAttemptId(),
              taskContext.attemptNumber(),
              SparkEnv.get().executorId());
      RssReportShuffleFetchFailureResponse response = client.reportShuffleFetchFailure(req);
      if (response.getReSubmitWholeStage()) {
        // since we are going to roll out the whole stage, mapIndex shouldn't matter, hence -1 is
        // provided.
        FetchFailedException ffe =
            RssSparkShuffleUtils.createFetchFailedException(
                builder.shuffleId, -1, builder.partitionId, e);
        return new RssException(ffe);
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
