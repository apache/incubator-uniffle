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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.uniffle.client.api.ShuffleServerClient;
import org.apache.uniffle.client.factory.ShuffleServerClientFactory;
import org.apache.uniffle.client.util.ClientType;
import org.apache.uniffle.common.ShuffleServerInfo;
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
    if (StorageType.HDFS.name().equals(request.getStorageType())) {
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
          request.getHadoopConf());
    } else if (StorageType.LOCALFILE.name().equals(request.getStorageType())) {
      List<ShuffleServerInfo> shuffleServerInfoList = request.getShuffleServerInfoList();
      List<ShuffleServerClient> shuffleServerClients = shuffleServerInfoList.stream().map(
          ssi -> ShuffleServerClientFactory.getInstance().getShuffleServerClient(ClientType.GRPC.name(), ssi)).collect(
          Collectors.toList());
      return new LocalFileQuorumClientReadHandler(request.getAppId(), request.getShuffleId(), request.getPartitionId(),
          request.getIndexReadLimit(), request.getPartitionNumPerRange(), request.getPartitionNum(),
          request.getReadBufferSize(), request.getExpectBlockIds(), request.getProcessBlockIds(),
          shuffleServerClients);
    } else if (StorageType.LOCALFILE_HDFS.name().equals(request.getStorageType())) {
      List<ShuffleServerInfo> shuffleServerInfoList = request.getShuffleServerInfoList();
      List<ShuffleServerClient> shuffleServerClients = shuffleServerInfoList.stream().map(
          ssi -> ShuffleServerClientFactory.getInstance().getShuffleServerClient(
              ClientType.GRPC.name(), ssi)).collect(
          Collectors.toList());
      return new ComposedClientReadHandler(() -> {
        return new LocalFileQuorumClientReadHandler(
            request.getAppId(),
            request.getShuffleId(),
            request.getPartitionId(),
            request.getIndexReadLimit(),
            request.getPartitionNumPerRange(),
            request.getPartitionNum(),
            request.getReadBufferSize(),
            request.getExpectBlockIds(),
            request.getProcessBlockIds(),
            shuffleServerClients);
      }, () -> {
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
            request.getHadoopConf());
      });
    } else if (StorageType.MEMORY_LOCALFILE.name().equals(request.getStorageType())) {
      List<ShuffleServerInfo> shuffleServerInfoList = request.getShuffleServerInfoList();
      List<ShuffleServerClient> shuffleServerClients = shuffleServerInfoList.stream().map(
          ssi -> ShuffleServerClientFactory.getInstance().getShuffleServerClient(
              ClientType.GRPC.name(), ssi)).collect(
          Collectors.toList());
      ClientReadHandler memoryClientReadHandler = new MemoryQuorumClientReadHandler(
          request.getAppId(),
          request.getShuffleId(),
          request.getPartitionId(),
          request.getReadBufferSize(),
          shuffleServerClients,
          request.getProcessBlockIds());
      ClientReadHandler localClientReadHandler = new LocalFileQuorumClientReadHandler(request.getAppId(),
          request.getShuffleId(), request.getPartitionId(), request.getIndexReadLimit(),
          request.getPartitionNumPerRange(), request.getPartitionNum(),
          request.getReadBufferSize(), request.getExpectBlockIds(), request.getProcessBlockIds(),
          shuffleServerClients);
      return new ComposedClientReadHandler(memoryClientReadHandler, localClientReadHandler);
    } else if (StorageType.MEMORY_HDFS.name().equals(request.getStorageType())) {
      List<ShuffleServerInfo> shuffleServerInfoList = request.getShuffleServerInfoList();
      List<ShuffleServerClient> shuffleServerClients = shuffleServerInfoList.stream().map(
          ssi -> ShuffleServerClientFactory.getInstance().getShuffleServerClient(
              ClientType.GRPC.name(), ssi)).collect(
          Collectors.toList());
      return new ComposedClientReadHandler(() -> {
        return new MemoryQuorumClientReadHandler(
          request.getAppId(),
          request.getShuffleId(),
          request.getPartitionId(),
          request.getReadBufferSize(),
          shuffleServerClients,
          request.getProcessBlockIds());
      }, () -> {
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
            request.getHadoopConf());
      });
    } else if (StorageType.MEMORY_LOCALFILE_HDFS.name().equals(request.getStorageType())) {
      List<ShuffleServerInfo> shuffleServerInfoList = request.getShuffleServerInfoList();
      List<ShuffleServerClient> shuffleServerClients = shuffleServerInfoList.stream().map(
          ssi -> ShuffleServerClientFactory.getInstance().getShuffleServerClient(
              ClientType.GRPC.name(), ssi)).collect(
          Collectors.toList());
      return new ComposedClientReadHandler(() -> {
        return new MemoryQuorumClientReadHandler(
            request.getAppId(),
            request.getShuffleId(),
            request.getPartitionId(),
            request.getReadBufferSize(),
            shuffleServerClients,
            request.getProcessBlockIds());
      }, () -> {
        return new LocalFileQuorumClientReadHandler(request.getAppId(),
            request.getShuffleId(), request.getPartitionId(), request.getIndexReadLimit(),
            request.getPartitionNumPerRange(), request.getPartitionNum(),
            request.getReadBufferSize(), request.getExpectBlockIds(), request.getProcessBlockIds(),
            shuffleServerClients);
      }, () -> {
        return  new HdfsClientReadHandler(
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
            request.getHadoopConf());
      });
    } else {
      throw new UnsupportedOperationException(
          "Doesn't support storage type for client read handler:" + request.getStorageType());
    }
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
