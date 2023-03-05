package org.apache.uniffle.client.netty;

import com.google.common.collect.Maps;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.network.client.RpcResponseCallback;
import org.apache.uniffle.common.network.protocol.ResponseMessage;
import org.apache.uniffle.common.network.utils.NettyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class TransportResponseHandler extends ChannelInboundHandlerAdapter {
  private static final Logger logger = LoggerFactory.getLogger(TransportResponseHandler.class);
  private Channel channel;
  private Map<Long, RpcResponseCallback> outstandingRpcs;
  private final long requestTimeoutNs;
  private final AtomicLong timeOfLastRequestNs;
  private final boolean closeIdleConnections;
  private TransportClient client;

  public TransportResponseHandler(Channel channel, long requestTimeoutMs, boolean closeIdleConnections) {
    this.channel = channel;
    outstandingRpcs = Maps.newConcurrentMap();
    this.requestTimeoutNs = requestTimeoutMs * 1000L * 1000;
    this.timeOfLastRequestNs = new AtomicLong(0);
    this.closeIdleConnections = closeIdleConnections;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (msg instanceof ResponseMessage) {
        ResponseMessage responseMessage = (ResponseMessage) msg;
      RpcResponseCallback listener = outstandingRpcs.get(responseMessage.getRequestId());
      listener.onSuccess(msg);
    } else {
      throw new RssException("receive unexpected message!");
    }
    super.channelRead(ctx, msg);
  }

  /** Returns total number of outstanding requests (fetch requests + rpcs) */
  public int numOutstandingRequests() {
    return outstandingRpcs.size();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (numOutstandingRequests() > 0) {
      String remoteAddress = NettyUtils.getRemoteAddress(channel);
      logger.error(
          "Still have {} requests outstanding when connection from {} is closed",
          numOutstandingRequests(),
          remoteAddress);
      failOutstandingRequests(cause);
    }
  }

  private void failOutstandingRequests(Throwable cause) {
    for (Map.Entry<Long, RpcResponseCallback> entry : outstandingRpcs.entrySet()) {
      try {
        entry.getValue().onFailure(cause);
      } catch (Exception e) {
        logger.warn("RpcResponseCallback.onFailure throws exception", e);
      }
    }

    // It's OK if new fetches appear, as they will fail immediately.
    outstandingRpcs.clear();
  }

  public void addResponseCallback(long requestId, RpcResponseCallback callback) {
    outstandingRpcs.put(requestId, callback);
  }

  public RpcResponseCallback getResponseCallback(long requestId) {
    return outstandingRpcs.get(requestId);
  }

  public void removeRpcRequest(long requestId) {
    outstandingRpcs.remove(requestId);
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof IdleStateEvent) {
      IdleStateEvent e = (IdleStateEvent) evt;
      synchronized (this) {
        boolean isActuallyOverdue =
            System.nanoTime() - getTimeOfLastRequestNs() > requestTimeoutNs;
        if (e.state() == IdleState.ALL_IDLE && isActuallyOverdue) {
          if (numOutstandingRequests() > 0) {
            String address = NettyUtils.getRemoteAddress(ctx.channel());
            logger.error(
                "Connection to {} has been quiet for {} ms while there are outstanding "
                    + "requests.",
                address,
                requestTimeoutNs / 1000 / 1000);
          }
          if (closeIdleConnections) {
            // While CloseIdleConnections is enable, we also close idle connection
            client.timeOut();
            ctx.close();
          }
        }
      }
    }
    ctx.fireUserEventTriggered(evt);
  }

  public long getTimeOfLastRequestNs() {
    return timeOfLastRequestNs.get();
  }

  public void updateTimeOfLastRequest() {
    timeOfLastRequestNs.set(System.nanoTime());
  }

  public void setClient(TransportClient client) {
    this.client = client;
  }
}
