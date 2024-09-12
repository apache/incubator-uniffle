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

package org.apache.uniffle.server.storage;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.server.ShuffleServerMetrics;
import org.apache.uniffle.server.event.PurgeEvent;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.factory.ShuffleHandlerFactory;
import org.apache.uniffle.storage.handler.AsynchronousDeleteEvent;
import org.apache.uniffle.storage.handler.api.ShuffleDeleteHandler;
import org.apache.uniffle.storage.request.CreateShuffleDeleteHandlerRequest;
import org.apache.uniffle.storage.util.StorageType;

public class LocalDeletionStrategy extends AbstractDeletionStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(LocalDeletionStrategy.class);

  public LocalDeletionStrategy() {
    Runnable twoPhasesDeletionTask =
        () -> {
          while (true) {
            AsynchronousDeleteEvent asynchronousDeleteEvent = null;
            ShuffleDeleteHandler deleteHandler =
                ShuffleHandlerFactory.getInstance()
                    .createShuffleDeleteHandler(
                        new CreateShuffleDeleteHandlerRequest(
                            StorageType.LOCALFILE.name(), new Configuration()));
            try {
              asynchronousDeleteEvent = twoPhasesDeletionEventQueue.take();
              deleteHandler.delete(
                  asynchronousDeleteEvent.getNeedDeleteRenamePaths(),
                  asynchronousDeleteEvent.getAppId(),
                  asynchronousDeleteEvent.getUser());
            } catch (Exception e) {
              if (asynchronousDeleteEvent != null) {
                LOG.error(
                    "Delete Paths of {} failed.",
                    asynchronousDeleteEvent.getNeedDeleteRenamePaths(),
                    e);
              } else {
                LOG.error("Failed to delete a directory in hadoopTwoPhasesDeletionThread.", e);
              }
            }
          }
        };
    twoPhasesDeletionThread = new Thread(twoPhasesDeletionTask);
    twoPhasesDeletionThread.setName("hadoopTwoPhasesDeletionThread");
    twoPhasesDeletionThread.setDaemon(true);
  }

  @Override
  void deleteShuffleData(List<String> deletePaths, Storage storage, PurgeEvent event) {
    // delete shuffle data for application
    ShuffleDeleteHandler deleteHandler =
        ShuffleHandlerFactory.getInstance()
            .createShuffleDeleteHandler(
                new CreateShuffleDeleteHandlerRequest(
                    StorageType.LOCALFILE.name(), new Configuration()));
    String appId = event.getAppId();
    if (event.isTwoPhasesDeletion()) {
      AsynchronousDeleteEvent asynchronousDeleteEvent =
          new AsynchronousDeleteEvent(
              appId, event.getUser(), null, event.getShuffleIds(), deletePaths);
      deleteHandler.moveToTemp(asynchronousDeleteEvent);
      boolean isSucess = twoPhasesDeletionEventQueue.offer(asynchronousDeleteEvent);
      if (!isSucess) {
        LOG.warn(
            "Remove the case where the clearNeedDeleteHadoopPathThread queue is full and cannot accept elements.");
        ShuffleServerMetrics.counterLocalTwoPhasesDeletionFaileTd.inc();
      }
    } else {
      deleteHandler.delete(
          deletePaths.toArray(new String[deletePaths.size()]), appId, event.getUser());
    }
  }
}
