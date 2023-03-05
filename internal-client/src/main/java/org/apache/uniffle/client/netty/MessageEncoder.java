package org.apache.uniffle.client.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.uniffle.common.network.protocol.RequestMessage;
import org.apache.uniffle.common.network.protocol.SendShuffleDataMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MessageEncoder extends ChannelOutboundHandlerAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(ChannelOutboundHandlerAdapter.class);
  private final static int INIT_BUFFER = 64 * 1024;

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
    ByteBuf buf = ctx.alloc().buffer(INIT_BUFFER);
    RequestMessage requestMessage = (RequestMessage) msg;
//    LOG.info("SSSSSS: MessageEncoder requestId={}", ((SendShuffleDataMessage) requestMessage).requestId);
    requestMessage.serialize(buf);
    ctx.writeAndFlush(buf);
  }

  //  @Override
//  protected void encode(ChannelHandlerContext ctx, RequestMessage msg, ByteBuf out) {
//    msg.serialize(out);
//  }
}
