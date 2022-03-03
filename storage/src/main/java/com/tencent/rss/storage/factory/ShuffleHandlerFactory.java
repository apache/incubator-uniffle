/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available. 
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.rss.storage.factory;

import java.util.List;
import java.util.stream.Collectors;

import com.tencent.rss.client.api.ShuffleServerClient;
import com.tencent.rss.client.factory.ShuffleServerClientFactory;
import com.tencent.rss.client.util.ClientType;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.storage.handler.api.ClientReadHandler;
import com.tencent.rss.storage.handler.api.ShuffleDeleteHandler;
import com.tencent.rss.storage.handler.impl.ComposedClientReadHandler;
import com.tencent.rss.storage.handler.impl.HdfsClientReadHandler;
import com.tencent.rss.storage.handler.impl.HdfsShuffleDeleteHandler;
import com.tencent.rss.storage.handler.impl.LocalFileDeleteHandler;
import com.tencent.rss.storage.handler.impl.LocalFileQuorumClientReadHandler;
import com.tencent.rss.storage.handler.impl.MemoryQuorumClientReadHandler;
import com.tencent.rss.storage.handler.impl.UploadedHdfsClientReadHandler;
import com.tencent.rss.storage.request.CreateShuffleDeleteHandlerRequest;
import com.tencent.rss.storage.request.CreateShuffleReadHandlerRequest;
import com.tencent.rss.storage.util.StorageType;

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
    } else if (StorageType.LOCALFILE_HDFS_2.name().equals(request.getStorageType())) {
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
      }, () -> {
        return new UploadedHdfsClientReadHandler(
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
          shuffleServerClients);
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
            shuffleServerClients);
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
