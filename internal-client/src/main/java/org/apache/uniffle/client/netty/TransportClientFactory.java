package org.apache.uniffle.client.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.uniffle.common.network.utils.IOMode;
import org.apache.uniffle.common.network.utils.JavaUtils;
import org.apache.uniffle.common.network.utils.NettyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class TransportClientFactory implements Closeable {
  /** A simple data structure to track the pool of clients between two peer nodes. */
  private static class ClientPool {
    TransportClient[] clients;
    Object[] locks;

    ClientPool(int size) {
      clients = new TransportClient[size];
      locks = new Object[size];
      for (int i = 0; i < size; i++) {
        locks[i] = new Object();
      }
    }
  }

  /** Random number generator for picking connections between peers. */
  private final Random rand;

  private final int numConnectionsPerPeer;
  private final int connectionTimeout;
  private final ConcurrentHashMap<SocketAddress, ClientPool> connectionPool;

  private final Class<? extends Channel> socketChannelClass;
  private EventLoopGroup workerGroup;
  private PooledByteBufAllocator pooledAllocator;
  private volatile static TransportClientFactory _instance;
  private static final Logger logger = LoggerFactory.getLogger(TransportClientFactory.class);

  public TransportClientFactory(int numConnectionsPerPeer, IOMode ioMode, int clientThreads, boolean preferDirectBufs, int connectionTimeout) {
    this.connectionPool = new ConcurrentHashMap<>();
    this.numConnectionsPerPeer = numConnectionsPerPeer;
    this.connectionTimeout = connectionTimeout;
    this.rand = new Random();

    this.socketChannelClass = NettyUtils.getClientChannelClass(ioMode);
    logger.info("mode " + ioMode + " threads " + clientThreads);
    this.workerGroup =
        NettyUtils.createEventLoop(ioMode, clientThreads, "netty-client");
    this.pooledAllocator =
        NettyUtils.createPooledByteBufAllocator(
            preferDirectBufs, false /* allowCache */, clientThreads);
  }

  public static TransportClientFactory getInstance() {
    if(_instance == null){
      synchronized (TransportClientFactory.class) {
        if(_instance == null) {
          _instance = new TransportClientFactory(2, IOMode.NIO, 0, true, 60000);
        }
      }
    }
    return _instance;
  }

  /**
   * Create a completely new {@link TransportClient} to the given remote host / port. This
   * connection is not pooled.
   *
   * <p>As with {@link #createClient(String, int)}, this method is blocking.
   */
  private TransportClient internalCreateClient(InetSocketAddress address)
      throws IOException, InterruptedException {
    Bootstrap bootstrap = new Bootstrap();
    bootstrap
        .group(workerGroup)
        .channel(socketChannelClass)
        // Disable Nagle's Algorithm since we don't want packets to wait
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectionTimeout)
        .option(ChannelOption.ALLOCATOR, pooledAllocator);

//    if (conf.receiveBuf() > 0) {
//      bootstrap.option(ChannelOption.SO_RCVBUF, conf.receiveBuf());
//    }
//
//    if (conf.sendBuf() > 0) {
//      bootstrap.option(ChannelOption.SO_SNDBUF, conf.sendBuf());
//    }

    final AtomicReference<TransportClient> clientRef = new AtomicReference<>();
    final AtomicReference<Channel> channelRef = new AtomicReference<>();

    bootstrap.handler(
        new ChannelInitializer<SocketChannel>() {
          @Override
          public void initChannel(SocketChannel ch) {
            TransportResponseHandler responseHandler = initializePipeline(ch);
            TransportClient client = new TransportClient(ch, responseHandler);
            responseHandler.setClient(client);
            clientRef.set(client);
            channelRef.set(ch);
          }
        });

    // Connect to the remote server
    ChannelFuture cf = bootstrap.connect(address);
    if (!cf.await(connectionTimeout)) {
      throw new IOException(
          String.format("Connecting to %s timed out (%s ms)", address, connectionTimeout));
    } else if (cf.cause() != null) {
      throw new IOException(String.format("Failed to connect to %s", address), cf.cause());
    }

    TransportClient client = clientRef.get();
    assert client != null : "Channel future completed successfully with null client";

    logger.debug("Connection to {} successful", address);

    return client;
  }

  public TransportClient createClient(String remoteHost, int remotePort, int partitionId)
      throws IOException, InterruptedException {
    // Get connection from the connection pool first.
    // If it is not found or not active, create a new one.
    // Use unresolved address here to avoid DNS resolution each time we creates a client.
    final InetSocketAddress unresolvedAddress =
        InetSocketAddress.createUnresolved(remoteHost, remotePort);

    // Create the ClientPool if we don't have it yet.
    ClientPool clientPool = connectionPool.get(unresolvedAddress);
    if (clientPool == null) {
      connectionPool.putIfAbsent(unresolvedAddress, new ClientPool(numConnectionsPerPeer));
      clientPool = connectionPool.get(unresolvedAddress);
    }

    int clientIndex =
        partitionId < 0 ? rand.nextInt(numConnectionsPerPeer) : partitionId % numConnectionsPerPeer;
    TransportClient cachedClient = clientPool.clients[clientIndex];

    if (cachedClient != null && cachedClient.isActive()) {
      // Make sure that the channel will not timeout by updating the last use time of the
      // handler. Then check that the client is still alive, in case it timed out before
      // this code was able to update things.
      //      TransportChannelHandler handler =
      //          cachedClient.getChannel().pipeline().get(TransportChannelHandler.class);
      //      synchronized (handler) {
      //        handler.getResponseHandler().updateTimeOfLastRequest();
      //      }
      cachedClient.getHandler().updateTimeOfLastRequest();

      if (cachedClient.isActive()) {
        logger.trace(
            "Returning cached connection to {}: {}", cachedClient.getSocketAddress(), cachedClient);
        return cachedClient;
      }
    }

    // If we reach here, we don't have an existing connection open. Let's create a new one.
    // Multiple threads might race here to create new connections. Keep only one of them active.
    final long preResolveHost = System.nanoTime();
    final InetSocketAddress resolvedAddress = new InetSocketAddress(remoteHost, remotePort);
    final long hostResolveTimeMs = (System.nanoTime() - preResolveHost) / 1000000;
    if (hostResolveTimeMs > 2000) {
      logger.warn("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs);
    } else {
      logger.trace("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs);
    }

    synchronized (clientPool.locks[clientIndex]) {
      cachedClient = clientPool.clients[clientIndex];

      if (cachedClient != null) {
        if (cachedClient.isActive()) {
          logger.trace("Returning cached connection to {}: {}", resolvedAddress, cachedClient);
          return cachedClient;
        } else {
          logger.info("Found inactive connection to {}, creating a new one.", resolvedAddress);
        }
      }
      clientPool.clients[clientIndex] = internalCreateClient(resolvedAddress);
      return clientPool.clients[clientIndex];
    }
  }

  public TransportClient createClient(String remoteHost, int remotePort)
      throws IOException, InterruptedException {
    return createClient(remoteHost, remotePort, -1);
  }

  public TransportResponseHandler initializePipeline(SocketChannel channel) {
    try {
      TransportResponseHandler responseHandler = new TransportResponseHandler(channel, connectionTimeout, true);
      channel
          .pipeline()
          .addLast("encoder", new MessageEncoder()) // out
          .addLast("decoder", new MessageDecoder()) // in
          .addLast(
              "idleStateHandler", new IdleStateHandler(0, 0, connectionTimeout / 1000))
          .addLast("responseHandler", responseHandler);
      return responseHandler;
    } catch (RuntimeException e) {
      logger.error("Error while initializing Netty pipeline", e);
      throw e;
    }
  }

  @Override
  public void close() {
    // Go through all clients and close them if they are active.
    for (ClientPool clientPool : connectionPool.values()) {
      for (int i = 0; i < clientPool.clients.length; i++) {
        TransportClient client = clientPool.clients[i];
        if (client != null) {
          clientPool.clients[i] = null;
          JavaUtils.closeQuietly(client);
        }
      }
    }
    connectionPool.clear();

    // SPARK-19147
    if (workerGroup != null && !workerGroup.isShuttingDown()) {
      workerGroup.shutdownGracefully();
    }
  }
}
