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

import java.nio.ByteBuffer;

import com.google.protobuf.ByteString;

import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.proto.RssProtos;

public class RssPartitionToShuffleServerResponse extends ClientResponse {
  private ByteBuffer shuffleHandleBytes;

  public RssPartitionToShuffleServerResponse(
      StatusCode statusCode, String message, ByteBuffer shuffleHandleBytes) {
    super(statusCode, message);
    this.shuffleHandleBytes = shuffleHandleBytes;
  }

  public ByteBuffer getShuffleHandleBytes() {
    return shuffleHandleBytes;
  }

  public static RssPartitionToShuffleServerResponse fromProto(
      RssProtos.PartitionToShuffleServerResponse response) {
    ByteString shuffleHandleBytes = response.getShuffleHandleInfoSerializableBytes();
    return new RssPartitionToShuffleServerResponse(
        StatusCode.valueOf(response.getStatus().name()),
        response.getMsg(),
        shuffleHandleBytes.asReadOnlyByteBuffer());
  }
}
