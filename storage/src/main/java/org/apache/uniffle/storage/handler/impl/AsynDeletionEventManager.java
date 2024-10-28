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

import java.util.concurrent.BlockingQueue;

import com.google.common.collect.Queues;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.storage.factory.ShuffleHandlerFactory;
import org.apache.uniffle.storage.handler.AsynDeletionEvent;
import org.apache.uniffle.storage.handler.api.ShuffleDeleteHandler;
import org.apache.uniffle.storage.request.CreateShuffleDeleteHandlerRequest;
import org.apache.uniffle.storage.util.StorageType;

public class AsynDeletionEventManager {
  private static final Logger LOG = LoggerFactory.getLogger(AsynDeletionEventManager.class);

  private static AsynDeletionEventManager INSTANCE;

  public static synchronized AsynDeletionEventManager getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new AsynDeletionEventManager();
    }
    return INSTANCE;
  }

  protected final BlockingQueue<AsynDeletionEvent> twoPhasesDeletionEventQueue =
      Queues.newLinkedBlockingQueue();
  protected Thread twoPhasesDeletionThread;

  public AsynDeletionEventManager() {
    Runnable twoPhasesDeletionTask =
        () -> {
          while (true) {
            AsynDeletionEvent asynDeletionEvent = null;
            try {
              asynDeletionEvent = twoPhasesDeletionEventQueue.take();
              if (asynDeletionEvent
                  .getStorageType()
                  .equalsIgnoreCase(StorageType.LOCALFILE.name())) {
                ShuffleDeleteHandler deleteHandler =
                    ShuffleHandlerFactory.getInstance()
                        .createShuffleDeleteHandler(
                            new CreateShuffleDeleteHandlerRequest(
                                StorageType.LOCALFILE.name(), new Configuration()));
                deleteHandler.delete(
                    asynDeletionEvent.getNeedDeleteRenamePaths(),
                    asynDeletionEvent.getAppId(),
                    asynDeletionEvent.getUser());
              } else if (asynDeletionEvent
                  .getStorageType()
                  .equalsIgnoreCase(StorageType.HDFS.name())) {
                ShuffleDeleteHandler deleteHandler =
                    ShuffleHandlerFactory.getInstance()
                        .createShuffleDeleteHandler(
                            new CreateShuffleDeleteHandlerRequest(
                                StorageType.HDFS.name(),
                                asynDeletionEvent.getConf(),
                                asynDeletionEvent.getShuffleServerId()));
                deleteHandler.delete(
                    asynDeletionEvent.getNeedDeleteRenamePaths(),
                    asynDeletionEvent.getAppId(),
                    asynDeletionEvent.getUser());
              }
            } catch (Exception e) {
              if (asynDeletionEvent != null) {
                LOG.error(
                    "Delete Paths of {} failed.", asynDeletionEvent.getNeedDeleteRenamePaths(), e);
              } else {
                LOG.error("Failed to delete a directory in twoPhasesDeletionThread.", e);
              }
            }
          }
        };
    twoPhasesDeletionThread = new Thread(twoPhasesDeletionTask);
    twoPhasesDeletionThread.setName("twoPhasesDeletionThread");
    twoPhasesDeletionThread.setDaemon(true);
    twoPhasesDeletionThread.start();
  }

  public synchronized boolean handlerDeletionQueue(AsynDeletionEvent asynDeletionEvent) {
    return twoPhasesDeletionEventQueue.offer(asynDeletionEvent);
  }
}
