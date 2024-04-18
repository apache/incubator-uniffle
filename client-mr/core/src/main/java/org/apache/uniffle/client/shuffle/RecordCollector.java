package org.apache.uniffle.client.shuffle;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;

class RecordCollector<K extends WritableComparable, V extends Writable> implements ReduceContext<K, V, K, V> {

  private final Iterator<Map.Entry<K, List<Record<K, V>>>> oldRecords;
  private final List<Record<K, V>> newRecords;

  private K currentKey;
  private List<Record<K, V>> currentValues;

  RecordCollector(Iterator<Map.Entry<K, List<Record<K, V>>>> oldRecords, List<Record<K, V>> newRecords) {
    this.oldRecords = oldRecords;
    this.newRecords = newRecords;
  }

  @Override
  public boolean nextKey() throws IOException, InterruptedException {
    boolean ret = oldRecords.hasNext();
    if (ret) {
      Map.Entry<K, List<Record<K, V>>> entry = oldRecords.next();
      currentKey = entry.getKey();
      currentValues = entry.getValue();
    }
    return ret;
  }

  @Override
  public Iterable<V> getValues() throws IOException, InterruptedException {
    Iterator<Record<K, V>> currentValuesIterator = currentValues.iterator();
    return new Iterable<V>() {
      @Override
      public Iterator<V> iterator() {
        return new Iterator<V>() {

          @Override
          public boolean hasNext() {
            return currentValuesIterator.hasNext();
          }

          @Override
          public V next() {
            return currentValuesIterator.next().getValue();
          }
        };
      }
    };
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return false;
  }

  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    return this.currentKey;
  }

  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public void write(K key, V value) throws IOException, InterruptedException {
    // we must deep copy the key and value
    // TODO: can we use byte directly?
    this.newRecords.add(Record.create(WritableUtils.deepCopy(key), WritableUtils.deepCopy(value)));
  }

  @Override
  public OutputCommitter getOutputCommitter() {
    return null;
  }

  @Override
  public TaskAttemptID getTaskAttemptID() {
    return null;
  }

  @Override
  public void setStatus(String s) {

  }

  @Override
  public String getStatus() {
    return null;
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public Counter getCounter(Enum<?> anEnum) {
    return null;
  }

  @Override
  public Counter getCounter(String s, String s1) {
    return null;
  }

  @Override
  public Configuration getConfiguration() {
    return null;
  }

  @Override
  public Credentials getCredentials() {
    return null;
  }

  @Override
  public JobID getJobID() {
    return null;
  }

  @Override
  public int getNumReduceTasks() {
    return 0;
  }

  @Override
  public Path getWorkingDirectory() throws IOException {
    return null;
  }

  @Override
  public Class<?> getOutputKeyClass() {
    return null;
  }

  @Override
  public Class<?> getOutputValueClass() {
    return null;
  }

  @Override
  public Class<?> getMapOutputKeyClass() {
    return null;
  }

  @Override
  public Class<?> getMapOutputValueClass() {
    return null;
  }

  @Override
  public String getJobName() {
    return null;
  }

  @Override
  public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
    return null;
  }

  @Override
  public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
    return null;
  }

  @Override
  public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
    return null;
  }

  @Override
  public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
    return null;
  }

  @Override
  public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
    return null;
  }

  @Override
  public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
    return null;
  }

  @Override
  public RawComparator<?> getSortComparator() {
    return null;
  }

  @Override
  public String getJar() {
    return null;
  }

  @Override
  public RawComparator<?> getCombinerKeyGroupingComparator() {
    return null;
  }

  @Override
  public RawComparator<?> getGroupingComparator() {
    return null;
  }

  @Override
  public boolean getJobSetupCleanupNeeded() {
    return false;
  }

  @Override
  public boolean getTaskCleanupNeeded() {
    return false;
  }

  @Override
  public boolean getProfileEnabled() {
    return false;
  }

  @Override
  public String getProfileParams() {
    return null;
  }

  @Override
  public Configuration.IntegerRanges getProfileTaskRange(boolean b) {
    return null;
  }

  @Override
  public String getUser() {
    return null;
  }

  @Override
  public boolean getSymlink() {
    return false;
  }

  @Override
  public Path[] getArchiveClassPaths() {
    return new Path[0];
  }

  @Override
  public URI[] getCacheArchives() throws IOException {
    return new URI[0];
  }

  @Override
  public URI[] getCacheFiles() throws IOException {
    return new URI[0];
  }

  @Override
  public Path[] getLocalCacheArchives() throws IOException {
    return new Path[0];
  }

  @Override
  public Path[] getLocalCacheFiles() throws IOException {
    return new Path[0];
  }

  @Override
  public Path[] getFileClassPaths() {
    return new Path[0];
  }

  @Override
  public String[] getArchiveTimestamps() {
    return new String[0];
  }

  @Override
  public String[] getFileTimestamps() {
    return new String[0];
  }

  @Override
  public int getMaxMapAttempts() {
    return 0;
  }

  @Override
  public int getMaxReduceAttempts() {
    return 0;
  }

  @Override
  public void progress() {

  }
}
