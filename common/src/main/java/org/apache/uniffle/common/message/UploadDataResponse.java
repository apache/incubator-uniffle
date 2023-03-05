package org.apache.uniffle.common.message;

import io.netty.buffer.ByteBuf;
import org.apache.uniffle.common.util.ByteBufUtils;
import org.apache.uniffle.common.util.MessageConstants;

public class UploadDataResponse extends BaseMessage {

  private StatusCode statusCode;
  private String retMessage;

  public UploadDataResponse(StatusCode statusCode, String retMessage) {
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
    buf.writeInt(statusCode.ordinal());
    ByteBufUtils.writeLengthAndString(buf, retMessage);
  }

  public static UploadDataResponse deserialize(ByteBuf buf) {
    StatusCode statusCode = StatusCode.valueOf(buf.readInt());
    String retMessage = ByteBufUtils.readLengthAndString(buf);
    return new UploadDataResponse(statusCode, retMessage);
  }
}
