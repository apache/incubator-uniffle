package org.apache.uniffle.client.shuffle.metrics;

public interface MetricsReporter {
  void incRecordsRead(long v);
}
