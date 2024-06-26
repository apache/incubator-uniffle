package org.apache.uniffle.common.exception;

public class StageRetryAbortException extends RuntimeException {

    public StageRetryAbortException(String message) {
        super(message);
    }
}
