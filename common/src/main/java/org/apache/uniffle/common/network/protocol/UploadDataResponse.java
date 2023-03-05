package org.apache.uniffle.common.network.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.ByteBufUtils;
import org.apache.uniffle.common.util.MessageConstants;

public class UploadDataResponse implements ResponseMessage {
  private long requestId;
  private StatusCode statusCode;
  private String retMessage;

  public UploadDataResponse() {}

  public UploadDataResponse(long requestId, StatusCode statusCode, String retMessage) {
    this.requestId = requestId;
    this.statusCode = statusCode;
    this.retMessage = retMessage;
  }

  public StatusCode getStatusCode() {
    return statusCode;
  }

  public String getRetMessage() {
    return retMessage;
  }

  @Override
  public String toString() {
    return "UploadDataResponse{" +
               "statusCode=" + statusCode +
               ", retMessage='" + retMessage + '\'' +
               '}';
  }

  @Override
  public int getMessageType() {
    return MessageConstants.MESSAGE_UploadDataResponse;
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeLong(requestId);
    buf.writeInt(statusCode.ordinal());
    ByteBufUtils.writeLengthAndString(buf, retMessage);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    requestId = buf.readLong();
    statusCode = StatusCode.valueOf(buf.readInt());
    retMessage = ByteBufUtils.readLengthAndString(buf);
  }

  @Override
  public long getRequestId() {
    return requestId;
  }
}
