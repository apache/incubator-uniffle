package org.apache.uniffle.client.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.network.protocol.UploadDataResponse;
import org.apache.uniffle.common.util.MessageConstants;

import java.util.List;

public class MessageDecoder extends ByteToMessageDecoder {
  private State state = State.READ_MESSAGE_HEADER;
  private byte status;
  private int messageType;
  private int messageLen;

//  @Override
//  public void channelRead(ChannelHandlerContext ctx, Object msg) {
//    ByteBuf in = (ByteBuf) msg;
//    switch (state) {
//      case READ_MESSAGE_HEADER:
//        if (in.readableBytes() < Byte.BYTES + Integer.BYTES + Integer.BYTES) {
//          return;
//        }
//        status = in.readByte();
//        if (status != MessageConstants.RESPONSE_STATUS_OK) {
//          throw new RssException("request failed! status: " + status);
//        }
//        messageType = in.readInt();
//        messageLen = in.readInt();
//        state = State.READ_MESSAGE;
//        return;
//      case READ_MESSAGE:
//        if (in.readableBytes() < messageLen) {
//          return;
//        }
//        if (messageType == MessageConstants.MESSAGE_UploadDataResponse) {
//          byte[] bytes = new byte[messageLen];
//          in.readBytes(bytes);
//          UploadDataResponse uploadDataResponse = new UploadDataResponse();
//          uploadDataResponse.deserialize(Unpooled.wrappedBuffer(bytes));
//          ctx.fireChannelRead(uploadDataResponse);
//        } else {
//          throw new RssException("not support message type!");
//        }
//        state = State.READ_MESSAGE_HEADER;
//        return;
//      default:
//        throw new RssException("not support State!");
//    }
//  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    super.channelInactive(ctx);
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    switch (state) {
      case READ_MESSAGE_HEADER:
        if (in.readableBytes() < Byte.BYTES + Integer.BYTES + Integer.BYTES) {
          return;
        }
        status = in.readByte();
        if (status != MessageConstants.RESPONSE_STATUS_OK) {
          throw new RssException("request failed! status: " + status);
        }
        messageType = in.readInt();
        messageLen = in.readInt();
        state = State.READ_MESSAGE;
        return;
      case READ_MESSAGE:
        if (in.readableBytes() < messageLen) {
          return;
        }
        byte[] bytes = new byte[messageLen];
        in.readBytes(bytes);
        if (messageType == MessageConstants.MESSAGE_UploadDataResponse) {
          UploadDataResponse uploadDataResponse = new UploadDataResponse();
          uploadDataResponse.deserialize(Unpooled.wrappedBuffer(bytes));
          out.add(uploadDataResponse);
        } else {
          throw new RssException("not support message type!");
        }
        state = State.READ_MESSAGE_HEADER;
        return;
      default:
        throw new RssException("not support State!");
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    super.exceptionCaught(ctx, cause);
  }

  private enum State {
    READ_MESSAGE_HEADER,
    READ_MESSAGE
  }
}
