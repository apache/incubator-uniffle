package org.apache.uniffle.common.util;

public class MessageConstants {

  public static final byte UPLOAD_DATA_MAGIC_BYTE = 'u';
  public static final byte GET_LOCAL_SHUFFLE_DATA_MAGIC_BYTE = 'f';
  public static final byte GET_IN_MEMORY_SHUFFLE_DATA_MAGIC_BYTE = 'i';

  public static final byte MESSAGE_UPLOAD_DATA_PARTITION_END = 50;
  public static final byte MESSAGE_UPLOAD_DATA_PARTITION_CONTINUE = 51;

  public static final byte RESPONSE_STATUS_OK = 20;
  public static final byte RESPONSE_STATUS_ERROR = 21;
  public final static byte RESPONSE_STATUS_UNSPECIFIED = 0;

  public static final int DEFAULT_SHUFFLE_DATA_MESSAGE_SIZE = 32 * 1024;
  public static final int MESSAGE_UPLOAD_DATA_END = -1;
  public static final int MESSAGE_TYPE_UPLOAD_DATA = 1;

  public final static int MESSAGE_UploadDataResponse = -101;
}
