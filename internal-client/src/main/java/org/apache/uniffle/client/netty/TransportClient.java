package org.apache.uniffle.client.netty;

import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.uniffle.common.network.client.RpcResponseCallback;
import org.apache.uniffle.common.network.protocol.SendShuffleDataMessage;
import org.apache.uniffle.common.network.utils.NettyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicLong;

public class TransportClient implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(TransportClient.class);

  private final Channel channel;
  private volatile boolean timedOut;
  private TransportResponseHandler handler;

  public TransportClient(Channel channel, TransportResponseHandler handler) {
    this.channel = Preconditions.checkNotNull(channel);
    this.handler = Preconditions.checkNotNull(handler);
    this.timedOut = false;
  }

  public Channel getChannel() {
    return channel;
  }

  public boolean isActive() {
    return !timedOut && (channel.isOpen() || channel.isActive());
  }

  public SocketAddress getSocketAddress() {
    return channel.remoteAddress();
  }

  @Override
  public void close() throws IOException {

  }

  public ChannelFuture sendShuffleData(SendShuffleDataMessage message, RpcResponseCallback callback) {
    if (logger.isTraceEnabled()) {
      logger.trace("Pushing data to {}", NettyUtils.getRemoteAddress(channel));
    }
    long requestId = requestId();
    handler.addResponseCallback(requestId, callback);
    message.requestId = requestId;
    RpcChannelListener listener = new RpcChannelListener(requestId, callback);
    return channel.writeAndFlush(message).addListener(listener);
  }

  private static final AtomicLong counter = new AtomicLong();

  public static long requestId() {
    return counter.getAndIncrement();
  }

  public void timeOut() {
    this.timedOut = true;
  }

  public TransportResponseHandler getHandler() {
    return handler;
  }

  public class StdChannelListener implements GenericFutureListener<Future<? super Void>> {
    final long startTime;
    final Object requestId;

    public StdChannelListener(Object requestId) {
      this.startTime = System.currentTimeMillis();
      this.requestId = requestId;
    }

    @Override
    public void operationComplete(Future<? super Void> future) throws Exception {
      if (future.isSuccess()) {
        if (logger.isTraceEnabled()) {
          long timeTaken = System.currentTimeMillis() - startTime;
          logger.trace(
              "Sending request {} to {} took {} ms",
              requestId,
              NettyUtils.getRemoteAddress(channel),
              timeTaken);
        }
      } else {
        String errorMsg =
            String.format(
                "Failed to send RPC %s to %s: %s, channel will be closed",
                requestId, NettyUtils.getRemoteAddress(channel), future.cause());
        logger.warn(errorMsg);
        channel.close();
        try {
          handleFailure(errorMsg, future.cause());
        } catch (Exception e) {
          logger.error("Uncaught exception in RPC response callback handler!", e);
        }
      }
    }

    protected void handleFailure(String errorMsg, Throwable cause) {
      logger.error("Error encountered " + errorMsg, cause);
    }
  }

  private class RpcChannelListener extends StdChannelListener {
    final long rpcRequestId;
    final RpcResponseCallback callback;

    RpcChannelListener(long rpcRequestId, RpcResponseCallback callback) {
      super("RPC " + rpcRequestId);
      this.rpcRequestId = rpcRequestId;
      this.callback = callback;
    }

    @Override
    protected void handleFailure(String errorMsg, Throwable cause) {
      handler.removeRpcRequest(rpcRequestId);
      callback.onFailure(new IOException(errorMsg, cause));
    }
  }
}
