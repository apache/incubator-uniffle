package org.apache.uniffle.common.message;

public enum StatusCode {
  SUCCESS(0),
  DOUBLE_REGISTER(1),
  NO_BUFFER(2),
  INVALID_STORAGE(3),
  NO_REGISTER(4),
  NO_PARTITION(5),
  INTERNAL_ERROR(6),
  TIMEOUT(7);

  private final int statusCode;

  StatusCode(int code) {
    this.statusCode = code;
  }

  public int statusCode() {
    return statusCode;
  }

  public static StatusCode valueOf(int value) {
    if(value == SUCCESS.statusCode){
      return SUCCESS;
    } else if(value == DOUBLE_REGISTER.statusCode){
      return DOUBLE_REGISTER;
    } else if(value == NO_BUFFER.statusCode){
      return NO_BUFFER;
    } else if(value == INVALID_STORAGE.statusCode){
      return INVALID_STORAGE;
    } else if(value == NO_REGISTER.statusCode){
      return NO_REGISTER;
    } else if(value == NO_PARTITION.statusCode){
      return NO_PARTITION;
    } else if(value == INTERNAL_ERROR.statusCode){
      return INTERNAL_ERROR;
    } else if(value == TIMEOUT.statusCode){
      return TIMEOUT;
    } else {
      throw new RuntimeException("Unknown value");
    }
  }
}
