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

package org.apache.uniffle.client.response;

import java.io.IOException;

import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.BlockIdSet;
import org.apache.uniffle.common.util.RoaringBitmapBlockIdSet;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.proto.RssProtos;

public class RssGetShuffleResultResponse extends ClientResponse {

  private BlockIdSet blockIdBitmap;

  public RssGetShuffleResultResponse(StatusCode statusCode, byte[] serializedBitmap)
      throws IOException {
    super(statusCode);
    blockIdBitmap = new RoaringBitmapBlockIdSet(RssUtils.deserializeBitMap(serializedBitmap));
  }

  public BlockIdSet getBlockIdBitmap() {
    return blockIdBitmap;
  }

  public static RssGetShuffleResultResponse fromProto(
      RssProtos.GetShuffleResultResponse rpcResponse) {
    try {
      return new RssGetShuffleResultResponse(
          StatusCode.fromProto(rpcResponse.getStatus()),
          rpcResponse.getSerializedBitmap().toByteArray());
    } catch (Exception e) {
      throw new RssException(e);
    }
  }

  public static RssGetShuffleResultResponse fromProto(
      RssProtos.GetShuffleResultForMultiPartResponse rpcResponse) {
    try {
      return new RssGetShuffleResultResponse(
          StatusCode.fromProto(rpcResponse.getStatus()),
          rpcResponse.getSerializedBitmap().toByteArray());
    } catch (Exception e) {
      throw new RssException(e);
    }
  }
}
