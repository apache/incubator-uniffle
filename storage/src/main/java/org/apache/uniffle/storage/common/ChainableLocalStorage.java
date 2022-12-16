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

package org.apache.uniffle.storage.common;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.storage.handler.api.ServerReadHandler;
import org.apache.uniffle.storage.handler.api.ShuffleWriteHandler;
import org.apache.uniffle.storage.request.CreateShuffleReadHandlerRequest;
import org.apache.uniffle.storage.request.CreateShuffleWriteHandlerRequest;

public class ChainableLocalStorage extends AbstractStorage {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChainableLocalStorage.class);

  private final List<LocalStorage> chainableStorages;
  private int size;

  public ChainableLocalStorage(@Nonnull LocalStorage localStorage) {
    this.chainableStorages = new ArrayList<>();
    chainableStorages.add(localStorage);
    this.size = 1;
  }

  public String getBasePath() {
    return chainableStorages.get(size - 1).getBasePath();
  }

  public boolean isCorrupted() {
    return chainableStorages.get(size - 1).isCorrupted();
  }

  @Override
  ShuffleWriteHandler newWriteHandler(CreateShuffleWriteHandlerRequest request) {
    return null;
  }

  @Override
  public ShuffleWriteHandler getOrCreateWriteHandler(CreateShuffleWriteHandlerRequest request) {
    return chainableStorages.get(size - 1).getOrCreateWriteHandler(request);
  }

  @Override
  public ServerReadHandler getOrCreateReadHandler(CreateShuffleReadHandlerRequest request) {
    int index = request.getStorageSeqIndex();
    return chainableStorages.get(index).getOrCreateReadHandler(request);
  }

  @Override
  protected ServerReadHandler newReadHandler(CreateShuffleReadHandlerRequest request) {
    return null;
  }

  @Override
  public boolean canWrite() {
    return chainableStorages.get(size - 1).canWrite();
  }

  @Override
  public boolean lockShuffleShared(String shuffleKey) {
    return chainableStorages.get(size - 1).lockShuffleShared(shuffleKey);
  }

  @Override
  public boolean unlockShuffleShared(String shuffleKey) {
    return chainableStorages.get(size - 1).unlockShuffleShared(shuffleKey);
  }

  @Override
  public void updateWriteMetrics(StorageWriteMetrics metrics) {
    chainableStorages.get(size - 1).updateWriteMetrics(metrics);
  }

  @Override
  public void updateReadMetrics(StorageReadMetrics metrics) {

  }

  @Override
  public void createMetadataIfNotExist(String shuffleKey) {
    chainableStorages.get(size - 1).createMetadataIfNotExist(shuffleKey);
  }

  @Override
  public String getStoragePath() {
    return chainableStorages.get(size - 1).getStoragePath();
  }

  @Override
  public String getStorageHost() {
    return chainableStorages.get(size - 1).getStorageHost();
  }

  public void removeTailStorage() {
    this.chainableStorages.remove(size - 1);
    this.size -= 1;
  }

  public List<LocalStorage> getChainableStorages() {
    return chainableStorages;
  }

  public void switchTo(LocalStorage newLocalStorage) {
    // If it's the used storage, it should be switched as the latest local storage to write.
    if (chainableStorages.contains(newLocalStorage)) {
      chainableStorages.remove(newLocalStorage);
    }
    chainableStorages.add(newLocalStorage);
  }
}
