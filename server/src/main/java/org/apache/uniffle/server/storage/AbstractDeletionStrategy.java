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
import java.util.concurrent.BlockingQueue;

import com.google.common.collect.Queues;

import org.apache.uniffle.server.event.PurgeEvent;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.handler.AsynchronousDeleteEvent;

public abstract class AbstractDeletionStrategy {

  protected final BlockingQueue<AsynchronousDeleteEvent> twoPhasesDeletionEventQueue =
      Queues.newLinkedBlockingQueue();
  protected Thread twoPhasesDeletionThread;

  abstract void deleteShuffleData(List<String> deletePaths, Storage storage, PurgeEvent event);
}
