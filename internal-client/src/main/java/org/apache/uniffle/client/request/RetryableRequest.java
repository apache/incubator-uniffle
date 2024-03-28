package org.apache.uniffle.client.request;

public interface RetryableRequest {
    int getRetryMax();
    long getRetryIntervalMax();
    String operationType();
}