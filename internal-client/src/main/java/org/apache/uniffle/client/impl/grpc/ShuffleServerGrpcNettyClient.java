package org.apache.uniffle.client.impl.grpc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.uniffle.client.netty.TransportClient;
import org.apache.uniffle.client.netty.TransportClientFactory;
import org.apache.uniffle.client.request.RssSendShuffleDataRequest;
import org.apache.uniffle.client.response.RssSendShuffleDataResponse;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.exception.NotRetryException;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.message.BaseMessage;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.message.UploadDataResponse;
import org.apache.uniffle.common.network.client.RpcResponseCallback;
import org.apache.uniffle.common.network.protocol.SendShuffleDataMessage;
import org.apache.uniffle.common.util.RetryUtils;
import org.apache.uniffle.common.util.SocketUtils;
import org.apache.uniffle.proto.RssProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.request.RssGetInMemoryShuffleDataRequest;
import org.apache.uniffle.client.request.RssGetShuffleDataRequest;
import org.apache.uniffle.client.response.RssGetInMemoryShuffleDataResponse;
import org.apache.uniffle.client.response.RssGetShuffleDataResponse;
import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.util.MessageConstants;

public class ShuffleServerGrpcNettyClient extends ShuffleServerGrpcClient {
  private static final Logger LOG = LoggerFactory.getLogger(ShuffleServerGrpcNettyClient.class);

  protected String connectionInfo = "";
  private int nettyPort;
  private TransportClientFactory clientFactory;

  public ShuffleServerGrpcNettyClient(String host, int grpcPort, int nettyPort) {
    super(host, grpcPort);
    this.nettyPort = nettyPort;
    this.clientFactory = TransportClientFactory.getInstance();
  }

  private void closeSocket(Socket socket, InputStream inputStream, OutputStream outputStream) {
    if (socket == null) {
      return;
    }

    try {
      if (outputStream != null) {
        outputStream.flush();
      }
    } catch (Throwable e) {
      LOG.warn("Hit exception when flushing output stream: " + connectionInfo, e);
    }

    try {
      if (outputStream != null) {
        outputStream.close();
      }
    } catch (Throwable e) {
      LOG.warn("Hit exception when closing output stream: " + connectionInfo, e);
    }

    try {
      if (inputStream != null) {
        inputStream.close();
      }
    } catch (Throwable e) {
      LOG.warn("Hit exception when closing input stream: " + connectionInfo, e);
    }

    try {
      socket.close();
    } catch (Throwable e) {
      LOG.warn("Hit exception when closing socket: " + connectionInfo, e);
    }

    socket = null;
  }

  private void connect() {
    try {
      Socket socket = connectSocket((int)rpcTimeout);
      InputStream inputStream = socket.getInputStream();
      OutputStream outputStream = socket.getOutputStream();
      LOG.info(String.format("Connecting to server: %s", connectionInfo));
      // connect shuffle server
      // write flag for fetch shuffle data
      write(outputStream, new byte[]{MessageConstants.UPLOAD_DATA_MAGIC_BYTE});
    } catch (IOException e){
      throw new RssException("connect failed", e);
    }
  }

//  @Override
//  public synchronized RssGetShuffleDataResponse getShuffleData(RssGetShuffleDataRequest request) {
//    try {
//      long start = System.currentTimeMillis();
//      LOG.debug(String.format("Connecting to server: %s", connectionInfo));
//      // connect shuffle server
//      socket = connectSocket(10000);
//      inputStream = socket.getInputStream();
//      outputStream = socket.getOutputStream();
//      // write flag for fetch shuffle data
//      write(outputStream, new byte[]{MessageConstants.GET_LOCAL_SHUFFLE_DATA_MAGIC_BYTE});
//      String appId = request.getAppId();
//      byte[] appIdBytes = appId.getBytes();
//      ByteBuf bufAppId = Unpooled.buffer(Integer.BYTES + appIdBytes.length);
//      // write length of appId and appId
//      bufAppId.writeInt(appId.length());
//      bufAppId.writeBytes(appIdBytes);
//      write(outputStream, bufAppId.array());
//      bufAppId.release();
//      // buf for shuffleId + partitionId + partitionNumPerRange + PartitionNum + ReadBufferSize
//      ByteBuf bufInt = Unpooled.buffer(6 * Integer.BYTES + Long.BYTES);
//      bufInt.writeInt(request.getShuffleId());
//      bufInt.writeInt(request.getPartitionId());
//      bufInt.writeInt(request.getPartitionNumPerRange());
//      bufInt.writeInt(request.getPartitionNum());
//      bufInt.writeInt(request.getLength());
//      bufInt.writeLong(request.getOffset());
//      write(outputStream, bufInt.array());
//      bufInt.release();
//
//      RssGetShuffleDataResponse response = null;
//      if (readStatus(inputStream) == MessageConstants.RESPONSE_STATUS_OK) {
//        byte[] shuffleData = readBytes(inputStream, request.getLength());
//        response = new RssGetShuffleDataResponse(ResponseStatusCode.SUCCESS, shuffleData);
//      } else {
//        throw new RuntimeException("Can't get shuffle data with " + connectionInfo
//                                       + " for appId[" + appId + "], shuffleId[" + request.getShuffleId() + "], partitionId["
//                                       + request.getPartitionId() + "]");
//      }
//      return response;
//    } catch (Exception e) {
//      throw new RuntimeException("Error happen when get shuffle data from shuffle server", e);
//    } finally {
//      closeSocket(socket, inputStream, outputStream);
//    }
//  }
//
//  @Override
//  public RssGetInMemoryShuffleDataResponse getInMemoryShuffleData(
//      RssGetInMemoryShuffleDataRequest request) {
//    long start = System.currentTimeMillis();
//    String appId = request.getAppId();
//    byte[] appIdBytes = appId.getBytes();
//    ByteBuf bufAppId = Unpooled.buffer(Integer.BYTES + appIdBytes.length);
//    // write length of appId and appId
//    bufAppId.writeInt(appId.length());
//    bufAppId.writeBytes(appIdBytes);
//    write(outputStream, bufAppId.array());
//    bufAppId.release();
//    // buf for shuffleId + partitionId + partitionNumPerRange + PartitionNum + ReadBufferSize
//    ByteBuf bufInt = Unpooled.buffer(3 * Integer.BYTES + Long.BYTES);
//    bufInt.writeInt(request.getShuffleId());
//    bufInt.writeInt(request.getPartitionId());
//    bufInt.writeInt(request.getReadBufferSize());
//    bufInt.writeLong(request.getLastBlockId());
//    write(outputStream, bufInt.array());
//    bufInt.release();
//
//    RssGetInMemoryShuffleDataResponse response = null;
//    if (readStatus(inputStream) == MessageConstants.RESPONSE_STATUS_OK) {
//      List<BufferSegment> bufferSegments = Lists.newArrayList();
//      int dataLength = readBufferSegments(inputStream, bufferSegments);
//      byte[] shuffleData = readBytes(inputStream, dataLength);
//      response = new RssGetInMemoryShuffleDataResponse(
//          ResponseStatusCode.SUCCESS, shuffleData, bufferSegments);
//      LOG.info("Successfully read shuffle data from memory with {} bytes", dataLength);
//    } else {
//      throw new RuntimeException("Can't get shuffle data with " + connectionInfo
//                                     + " for appId[" + appId + "], shuffleId[" + request.getShuffleId() + "], partitionId["
//                                     + request.getPartitionId() + "]");
//    }
//    return response;
//  }

  @Override
  public RssSendShuffleDataResponse sendShuffleData(RssSendShuffleDataRequest request) {
    TransportClient transportClient;
    try {
      transportClient = clientFactory.createClient(host, nettyPort);
    } catch (Exception e) {
      throw new RssException("createClient failed", e);
    }

    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleIdToBlocks = request.getShuffleIdToBlocks();
    AtomicBoolean isAllSuccess = new AtomicBoolean(true);
    CountDownLatch countDownLatch = new CountDownLatch(shuffleIdToBlocks.size());

    for (Map.Entry<Integer, Map<Integer, List<ShuffleBlockInfo>>> stb : shuffleIdToBlocks.entrySet()) {
      int shuffleId = stb.getKey();
      int size = 0;
      int blockNum = 0;
      for (Map.Entry<Integer, List<ShuffleBlockInfo>> ptb : stb.getValue().entrySet()) {
        for (ShuffleBlockInfo sbi : ptb.getValue()) {
          size += sbi.getSize();
          blockNum++;
        }
      }

      int allocateSize = size;
      int finalBlockNum = blockNum;
      try {
        long requireId = requirePreAllocation(allocateSize, request.getRetryMax(), request.getRetryIntervalMax());
        if (requireId == FAILED_REQUIRE_ID) {
          throw new RssException(String.format(
              "requirePreAllocation failed! size[%s], host[%s], port[%s]", allocateSize, host, port));
        }

        SendShuffleDataMessage sendShuffleDataMessage = new SendShuffleDataMessage(request.getAppId(), shuffleId, requireId, stb.getValue());
        RpcResponseCallback callback = new RpcResponseCallback() {
          @Override
          public void onSuccess(Object object) {
            org.apache.uniffle.common.network.protocol.UploadDataResponse response = (org.apache.uniffle.common.network.protocol.UploadDataResponse) object;
            if(response.getStatusCode() != StatusCode.SUCCESS) {
              String msg = "Can't send shuffle data with " + finalBlockNum
                               + " blocks to " + host + ":" + port
                               + ", statusCode=" + response.getStatusCode()
                               + ", errorMsg:" + response.getRetMessage();
              isAllSuccess.set(false);
              throw new RssException(msg);
            }
            countDownLatch.countDown();
          }

          @Override
          public void onFailure(Throwable e) {
            countDownLatch.countDown();
            isAllSuccess.set(false);
            throw new RssException("sendShuffleData failed!", e);
          }
        };
        transportClient.sendShuffleData(sendShuffleDataMessage, callback);
      } catch (Throwable throwable) {
        LOG.error("sendShuffleData failed!", throwable);
        isAllSuccess.set(false);
        break;
      }
    }
    try {
      countDownLatch.await(60L, TimeUnit.SECONDS);
    } catch (InterruptedException e ) {
      isAllSuccess.set(false);
      throw new RssException("countDownLatch interrupted", e);
    }

    RssSendShuffleDataResponse response;
    if (isAllSuccess.get()) {
      response = new RssSendShuffleDataResponse(StatusCode.SUCCESS);
    } else {
      response = new RssSendShuffleDataResponse(StatusCode.INTERNAL_ERROR);
    }
    return response;
  }

  private int readBufferSegments(InputStream inputStream, List<BufferSegment> segments) {
    int dataLength = 0;
    int segmentNum = readInt(inputStream);
    for (int i = 0; i < segmentNum; i++) {
      long blockId = readLong(inputStream);
      long offset = readLong(inputStream);
      int length = readInt(inputStream);
      int uncompressLength = readInt(inputStream);
      long crc = readLong(inputStream);
      long taskAttemptId = readLong(inputStream);
      segments.add(new BufferSegment(
          blockId, offset, length, uncompressLength, crc, taskAttemptId));
      dataLength += length;
    }
    return dataLength;
  }

  protected Socket connectSocket(int timeout) {
    Socket socket = null;
    long startTime = System.currentTimeMillis();
    int triedTimes = 0;
    try {
      Throwable lastException = null;
      while (System.currentTimeMillis() - startTime <= timeout) {
        if (triedTimes >= 1) {
          LOG.info(String
                       .format("Retrying connect to %s:%s, total retrying times: %s, elapsed milliseconds: %s", host,
                           nettyPort,
                           triedTimes, System.currentTimeMillis() - startTime));
        }
        triedTimes++;
        try {
          socket = new Socket();
          socket.setSoTimeout(timeout);
          socket.setTcpNoDelay(true);
          socket.connect(new InetSocketAddress(host, nettyPort), timeout);
          break;
        } catch (Exception socketException) {
          socket = null;
          lastException = socketException;
          LOG.warn(String.format("Failed to connect to %s:%s", host, nettyPort), socketException);
          Thread.sleep(500);
        }
      }

      if (socket == null) {
        if (lastException != null) {
          throw lastException;
        } else {
          throw new IOException(String.format("Failed to connect to %s:%s", host, nettyPort));
        }
      }

      connectionInfo = String.format("[%s -> %s (%s)]",
          socket.getLocalSocketAddress(),
          socket.getRemoteSocketAddress(),
          host);
    } catch (Throwable e) {
      long elapsedTime = System.currentTimeMillis() - startTime;
      String msg = String
                       .format("connectSocket failed after trying %s times for %s milliseconds (timeout set to %s): %s",
                           triedTimes, elapsedTime, timeout, connectionInfo);
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    }
    return socket;
  }

  protected void write(OutputStream outputStream, byte[] bytes) {
    try {
      outputStream.write(bytes);
      outputStream.flush();
    } catch (IOException e) {
      String logMsg = String.format("write failed: %s", connectionInfo);
      LOG.error(logMsg, e);
      throw new RuntimeException(logMsg, e);
    }
  }

  protected int readStatus(InputStream inputStream) {
    try {
      return inputStream.read();
    } catch (IOException e) {
      String logMsg = String.format("read status failed: %s", connectionInfo);
      LOG.error(logMsg, e);
      throw new RuntimeException(logMsg, e);
    }
  }

  private int readInt(InputStream stream) {
    byte[] bytes = readBytes(stream, Integer.BYTES);
    ByteBuf buf = Unpooled.wrappedBuffer(bytes);
    try {
      int value = buf.readInt();
      return value;
    } finally {
      buf.release();
    }
  }

  private long readLong(InputStream stream) {
    byte[] bytes = readBytes(stream, Long.BYTES);
    ByteBuf buf = Unpooled.wrappedBuffer(bytes);
    try {
      long value = buf.readLong();
      return value;
    } finally {
      buf.release();
    }
  }

  private byte[] readBytes(InputStream stream, int numBytes) {
    if (numBytes == 0) {
      return new byte[0];
    }

    byte[] result = new byte[numBytes];
    int readBytes = 0;
    while (readBytes < numBytes) {
      try {
        int numBytesToRead = numBytes - readBytes;
        int count = stream.read(result, readBytes, numBytesToRead);

        if (count == -1) {
          throw new RuntimeException(
              "Failed to read data bytes due to end of stream: "
                  + numBytesToRead);
        }

        readBytes += count;
      } catch (IOException e) {
        throw new RuntimeException("Failed to read data", e);
      }
    }

    return result;
  }

  protected <R extends BaseMessage> R readResponseMessage(InputStream inputStream, int messageId, Function<ByteBuf, R> deserializer) {
    int id = SocketUtils.readInt(inputStream);
    if (id != messageId) {
      throw new RssException(String.format("Expected message id: %s, actual message id: %s", messageId, id));
    }

    return readMessageLengthAndContent(inputStream, deserializer);
  }

  protected <R extends BaseMessage> R readMessageLengthAndContent(InputStream inputStream, Function<ByteBuf, R> deserializer) {
    int len = SocketUtils.readInt(inputStream);
    byte[] bytes = SocketUtils.readBytes(inputStream, len);

    ByteBuf buf = Unpooled.wrappedBuffer(bytes);
    try {
      R response = deserializer.apply(buf);
      return response;
    } finally {
      buf.release();
    }
  }
}
