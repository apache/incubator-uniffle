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

package org.apache.uniffle.server.netty.decoder;

import java.nio.ByteBuffer;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.util.MessageConstants;
import org.apache.uniffle.server.netty.message.UploadDataMessage;
import org.apache.uniffle.server.netty.util.NettyUtils;

public class UploadDataMessageDecoder extends ByteToMessageDecoder {

  private static final Logger LOG = LoggerFactory.getLogger(UploadDataMessageDecoder.class);

  private State state = State.READ_MAGIC_BYTE;
  private long requestId;
  private int requiredBytes = 0;
  private int partitionId;
  private long blockId;
  private long crc;
  private int uncompressLength;
  private int dataLength;
  private long taskAttemptId;
  private UploadDataMessage uploadDataMessage = new UploadDataMessage();
  private final ByteBuf shuffleDataBuffer;

  public UploadDataMessageDecoder(ByteBuf shuffleDataBuffer) {
    super();
    this.shuffleDataBuffer = shuffleDataBuffer;
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    super.channelInactive(ctx);
    if(shuffleDataBuffer != null) {
      shuffleDataBuffer.release();
    }
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
    if (in.readableBytes() == 0) {
      return;
    }

    switch (state) {
      case READ_MAGIC_BYTE:
        if (in.readableBytes() < Byte.BYTES) {
          return;
        }
        byte magicByte = in.readByte();
        switch (magicByte) {
          case MessageConstants.UPLOAD_DATA_MAGIC_BYTE:
            // start to process data upload
            state = State.READ_REQUEST_ID;
            return;
          default:
            String clientInfo = NettyUtils.getServerConnectionInfo(ctx);
            LOG.error(
                "Invalid magic byte {} from client {}",
                magicByte, clientInfo);
            ctx.close();
            LOG.error("Closed connection to client {}", clientInfo);
            return;
        }
      case READ_REQUEST_ID:
        if (in.readableBytes() < Long.BYTES) {
          return;
        }
        requestId = in.readLong();
//        LOG.info("SSSSSS: receive requestId=" + requestId);
        uploadDataMessage.setRequestId(requestId);
        state = State.READ_TASK_APPID_LEN;
        return;
      case READ_TASK_APPID_LEN:
        if (in.readableBytes() < Integer.BYTES) {
          return;
        }
        // read length of appId
        requiredBytes = in.readInt();
        if (requiredBytes < 0) {
          String clientInfo = NettyUtils.getServerConnectionInfo(ctx);
          LOG.error(
              "Invalid length of applicationId {} from client {}",
              requiredBytes, clientInfo);
          ctx.close();
          LOG.error("Closed connection to client {}", clientInfo);
          return;
        }
        state = State.READ_TASK_UPLOAD_INFO;
        return;
      case READ_TASK_UPLOAD_INFO:
        // appId + shuffleId + requireBufferId
        if (in.readableBytes() < requiredBytes + Integer.BYTES + Long.BYTES) {
          return;
        }
        uploadDataMessage.readMessageInfo(in, requiredBytes);
        state = State.READ_PARTITION_ID;
        return;
      case READ_PARTITION_ID:
        if (in.readableBytes() < Integer.BYTES) {
          return;
        }
        int tmpPartitionId = in.readInt();
        // check if there has no more data
        if (tmpPartitionId == MessageConstants.MESSAGE_UPLOAD_DATA_END) {
          // add message to process
          out.add(uploadDataMessage);
          resetData();
          state = State.READ_MAGIC_BYTE;
        } else {
          partitionId = tmpPartitionId;
          state = State.READ_BLOCK_INFO_START;
        }
        return;
      case READ_BLOCK_INFO_START:
        if (in.readableBytes() < Byte.BYTES) {
          return;
        }
        byte statusFlg = in.readByte();
        // check if there has no more data for current partition
        if (statusFlg == MessageConstants.MESSAGE_UPLOAD_DATA_PARTITION_END) {
          state = State.READ_PARTITION_ID;
        } else if (statusFlg == MessageConstants.MESSAGE_UPLOAD_DATA_PARTITION_CONTINUE) {
          state = State.READ_BLOCK_INFO;
        } else {
          throw new RuntimeException("Unexpected flag[" + statusFlg + "] in READ_BLOCK_INFO_START status");
        }
        // there has no data when come here for new partition
//        uploadDataMessage.addBlockData(partitionId, blockId, crc, uncompressLength, dataLength, taskAttemptId, shuffleDataBuffer);
//        blockId = -1;
//        crc = -1;
//        uncompressLength = 0;
//        dataLength = 0;
//        taskAttemptId = -1;
//        if (shuffleDataBuffer.readableBytes() > 0) {
//          // add block to message
//          uploadDataMessage.addBlockData(partitionId, blockId, crc, uncompressLength, dataLength, taskAttemptId, shuffleDataBuffer);
//        }
        return;
      case READ_BLOCK_INFO:
        // blockId + crc + uncompressLength + length
        if (in.readableBytes() < Long.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES + Long.BYTES) {
          return;
        }
        blockId = in.readLong();
        crc = in.readLong();
        uncompressLength = in.readInt();
        dataLength = in.readInt();
        taskAttemptId = in.readLong();

        if (dataLength == 0) {
          throw new RuntimeException("Unexpected 0 length for data block[" + blockId + "]");
        } else {
          // create ByteBuffer for new data block
          shuffleDataBuffer.clear();
          requiredBytes = dataLength;
          state = State.READ_BLOCK_DATA;
        }
        return;
      case READ_BLOCK_DATA:
        // read data to ByteBuffer
        int readableBytes = in.readableBytes();
        if (readableBytes < requiredBytes) {
          shuffleDataBuffer.ensureWritable(readableBytes);
          in.readBytes(shuffleDataBuffer, readableBytes);
          requiredBytes -= readableBytes;
        } else {
          shuffleDataBuffer.ensureWritable(requiredBytes);
          in.readBytes(shuffleDataBuffer, requiredBytes);
          requiredBytes = 0;
          uploadDataMessage.addBlockData(partitionId, blockId, crc, uncompressLength, dataLength, taskAttemptId, shuffleDataBuffer);
          state = State.READ_BLOCK_INFO_START;
        }
        return;
      default:
        throw new RuntimeException(String.format(
            "Should not get incoming data in state %s, client %s",
            state, NettyUtils.getServerConnectionInfo(ctx)));
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    String connectionInfo = NettyUtils.getServerConnectionInfo(ctx);
    String msg = "Got exception " + connectionInfo;
    LOG.warn(msg, cause);

    ctx.close();
  }

  private void resetData() {
    uploadDataMessage = new UploadDataMessage();
  }

  private enum State {
    READ_MAGIC_BYTE,
    READ_REQUEST_ID,
    READ_TASK_APPID_LEN,
    READ_TASK_UPLOAD_INFO,
    READ_PARTITION_ID,
    READ_BLOCK_INFO_START,
    READ_BLOCK_INFO,
    READ_BLOCK_DATA
  }
}
