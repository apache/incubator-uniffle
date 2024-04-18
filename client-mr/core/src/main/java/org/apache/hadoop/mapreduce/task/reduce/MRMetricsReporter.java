package org.apache.hadoop.mapreduce.task.reduce;

import org.apache.hadoop.mapred.Reporter;
import org.apache.uniffle.client.shuffle.metrics.MetricsReporter;

public class MRMetricsReporter implements MetricsReporter {

  Reporter reporter;

  public MRMetricsReporter(Reporter reporter) {
   this.reporter = reporter;
  }

  @Override
  public void incRecordsRead(long v) {
    this.reporter.incrCounter(RMRssShuffle.Counter.INPUT_RECORDS_PROCESSED, v);
  }
}
