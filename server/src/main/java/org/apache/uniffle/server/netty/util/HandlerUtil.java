package org.apache.uniffle.server.netty.util;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import org.apache.uniffle.common.message.BaseMessage;
import org.apache.uniffle.common.network.protocol.ResponseMessage;
import org.apache.uniffle.proto.RssProtos;
import org.apache.uniffle.common.rpc.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandlerUtil {
  private static final Logger logger = LoggerFactory.getLogger(HandlerUtil.class);

  public static ChannelFuture writeResponseMsg(ChannelHandlerContext ctx, byte responseStatus, ResponseMessage msg, boolean doWriteType) {
    ByteBuf serializedMsgBuf = ctx.alloc().buffer(1000);
    try {
      // need to serialize msg to get its length
      msg.serialize(serializedMsgBuf);
      ByteBuf responseMsgBuf = ctx.alloc().buffer(1000);
      try {
        responseMsgBuf.writeByte(responseStatus);
        if (doWriteType) {
          responseMsgBuf.writeInt(msg.getMessageType());
        }
        responseMsgBuf.writeInt(serializedMsgBuf.readableBytes());
        responseMsgBuf.writeBytes(serializedMsgBuf);
        return ctx.writeAndFlush(responseMsgBuf);
      } catch (Throwable ex) {
        logger.warn("Caught exception, releasing ByteBuf", ex);
        responseMsgBuf.release();
        throw ex;
      }
    } finally {
      serializedMsgBuf.release();
    }
  }

//
//  public static StatusCode valueOf(StatusCode code) {
//    switch (code) {
//      case SUCCESS:
//        return org.apache.uniffle.common.message.StatusCode.SUCCESS;
//      case DOUBLE_REGISTER:
//        return org.apache.uniffle.common.message.StatusCode.DOUBLE_REGISTER;
//      case NO_BUFFER:
//        return org.apache.uniffle.common.message.StatusCode.NO_BUFFER;
//      case INVALID_STORAGE:
//        return org.apache.uniffle.common.message.StatusCode.INVALID_STORAGE;
//      case NO_REGISTER:
//        return org.apache.uniffle.common.message.StatusCode.NO_REGISTER;
//      case NO_PARTITION:
//        return org.apache.uniffle.common.message.StatusCode.NO_PARTITION;
//      case TIMEOUT:
//        return org.apache.uniffle.common.message.StatusCode.TIMEOUT;
//      default:
//        return org.apache.uniffle.common.message.StatusCode.INTERNAL_ERROR;
//    }
//  }
}
