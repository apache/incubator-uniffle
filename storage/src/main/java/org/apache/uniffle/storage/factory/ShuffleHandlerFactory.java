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

package org.apache.uniffle.storage.factory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.apache.uniffle.client.api.ShuffleServerClient;
import org.apache.uniffle.client.factory.ShuffleServerClientFactory;
import org.apache.uniffle.client.util.ClientType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.storage.handler.api.ClientReadHandler;
import org.apache.uniffle.storage.handler.api.ShuffleDeleteHandler;
import org.apache.uniffle.storage.handler.impl.ComposedClientReadHandler;
import org.apache.uniffle.storage.handler.impl.HdfsClientReadHandler;
import org.apache.uniffle.storage.handler.impl.HdfsShuffleDeleteHandler;
import org.apache.uniffle.storage.handler.impl.LocalFileDeleteHandler;
import org.apache.uniffle.storage.handler.impl.LocalFileQuorumClientReadHandler;
import org.apache.uniffle.storage.handler.impl.MemoryQuorumClientReadHandler;
import org.apache.uniffle.storage.request.CreateShuffleDeleteHandlerRequest;
import org.apache.uniffle.storage.request.CreateShuffleReadHandlerRequest;
import org.apache.uniffle.storage.util.StorageType;

public class ShuffleHandlerFactory {

  private static ShuffleHandlerFactory INSTANCE;

  private ShuffleHandlerFactory() {
  }

  public static synchronized ShuffleHandlerFactory getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new ShuffleHandlerFactory();
    }
    return INSTANCE;
  }

  public ClientReadHandler createShuffleReadHandler(CreateShuffleReadHandlerRequest request) {
    String storageType = request.getStorageType();
    StorageType type = StorageType.valueOf(storageType);

    if (StorageType.MEMORY == type) {
      throw new UnsupportedOperationException(
          "Doesn't support storage type for client read handler:" + storageType);
    }

    if (StorageType.HDFS == type) {
      return getHdfsClientReadHandler(request);
    }
    if (StorageType.LOCALFILE == type) {
      return getLocalfileClientReaderHandler(request);
    }

    List<ClientReadHandler> handlers = new ArrayList<>();
    if (StorageType.withMemory(type)) {
      handlers.add(
          getMemoryClientReadHandler(request)
      );
    }
    if (StorageType.withLocalfile(type)) {
      handlers.add(
          getLocalfileClientReaderHandler(request)
      );
    }
    if (StorageType.withHDFS(type)) {
      handlers.add(
          getHdfsClientReadHandler(request)
      );
    }
    if (handlers.isEmpty()) {
      throw new RssException("This should not happen due to the unknown storage type: " + storageType);
    }

    Callable<ClientReadHandler>[] callables =
        handlers
            .stream()
            .map(x -> (Callable<ClientReadHandler>) () -> x)
            .collect(Collectors.toList())
            .toArray(new Callable[handlers.size()]);
    return new ComposedClientReadHandler(callables);
  }

  private ClientReadHandler getMemoryClientReadHandler(CreateShuffleReadHandlerRequest request) {
    List<ShuffleServerInfo> shuffleServerInfoList = request.getShuffleServerInfoList();
    List<ShuffleServerClient> shuffleServerClients = shuffleServerInfoList.stream().map(
        ssi -> ShuffleServerClientFactory.getInstance().getShuffleServerClient(
            ClientType.GRPC.name(), ssi, request.getConnectionOptions())).collect(
        Collectors.toList());
    ClientReadHandler memoryClientReadHandler = new MemoryQuorumClientReadHandler(
        request.getAppId(),
        request.getShuffleId(),
        request.getPartitionId(),
        request.getReadBufferSize(),
        shuffleServerClients);
    return memoryClientReadHandler;
  }

  private ClientReadHandler getLocalfileClientReaderHandler(CreateShuffleReadHandlerRequest request) {
    List<ShuffleServerInfo> shuffleServerInfoList = request.getShuffleServerInfoList();
    List<ShuffleServerClient> shuffleServerClients = shuffleServerInfoList
        .stream()
        .map(
            ssi -> ShuffleServerClientFactory
                .getInstance()
                .getShuffleServerClient(ClientType.GRPC.name(), ssi, request.getConnectionOptions())
        )
        .collect(Collectors.toList());
    return new LocalFileQuorumClientReadHandler(
        request.getAppId(), request.getShuffleId(), request.getPartitionId(),
        request.getIndexReadLimit(), request.getPartitionNumPerRange(), request.getPartitionNum(),
        request.getReadBufferSize(), request.getExpectBlockIds(), request.getProcessBlockIds(),
        shuffleServerClients, request.getDistributionType(), request.getExpectTaskIds()
    );
  }

  private ClientReadHandler getHdfsClientReadHandler(CreateShuffleReadHandlerRequest request) {
    return new HdfsClientReadHandler(
        request.getAppId(),
        request.getShuffleId(),
        request.getPartitionId(),
        request.getIndexReadLimit(),
        request.getPartitionNumPerRange(),
        request.getPartitionNum(),
        request.getReadBufferSize(),
        request.getExpectBlockIds(),
        request.getProcessBlockIds(),
        request.getStorageBasePath(),
        request.getHadoopConf(),
        request.getDistributionType(),
        request.getExpectTaskIds()
    );
  }

  public ShuffleDeleteHandler createShuffleDeleteHandler(CreateShuffleDeleteHandlerRequest request) {
    if (StorageType.HDFS.name().equals(request.getStorageType())) {
      return new HdfsShuffleDeleteHandler(request.getConf());
    } else if (StorageType.LOCALFILE.name().equals(request.getStorageType())) {
      return new LocalFileDeleteHandler();
    } else {
      throw new UnsupportedOperationException("Doesn't support storage type for shuffle delete handler:"
          + request.getStorageType());
    }
  }
}
