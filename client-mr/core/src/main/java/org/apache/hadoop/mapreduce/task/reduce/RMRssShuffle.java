package org.apache.hadoop.mapreduce.task.reduce;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.ShuffleConsumerPlugin;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RssMRConfig;
import org.apache.hadoop.mapreduce.RssMRUtils;
import org.apache.hadoop.util.Progress;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.shuffle.MRCombiner;
import org.apache.uniffle.client.shuffle.reader.KeyValueReader;
import org.apache.uniffle.client.shuffle.reader.RMRecordsReader;
import org.apache.uniffle.client.shuffle.writer.Combiner;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.serializer.SerializerFactory;
import org.apache.uniffle.common.serializer.SerializerInstance;
import org.apache.uniffle.common.util.ThreadUtils;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class RMRssShuffle<K, V> implements ShuffleConsumerPlugin<K, V>, ExceptionReporter {

  private static final int MAX_EVENTS_TO_FETCH = 10000;

  private static final Log LOG = LogFactory.getLog(RMRssShuffle.class);

  enum Counter { INPUT_RECORDS_PROCESSED }

  private String appId;
  private int appAttemptId;
  private int partitionId;

  private Context<K, V> context;
  private org.apache.hadoop.mapreduce.TaskAttemptID reduceId;
  private JobConf mrJobConf;
  private Configuration rssJobConf;
  private TaskStatus taskStatus;
  private Task reduceTask; // Used for status updates
  private TaskUmbilicalProtocol umbilical;

  private RssConf rssConf;
  private RecordsRelayer relayer;
  private Class keyClass;
  private Class valueClass;
  private RawComparator rawComparator;
  private SerializerInstance keySerializer;
  private SerializerInstance valueSerializer;
  private RMRecordsReader reader;
  Set<ShuffleServerInfo> serverInfoSet;
  private ScheduledExecutorService scheduledExecutorService;

  @Override
  public void init(Context<K, V> context) {
    LOG.info("use RMRssShuffle");
    this.context = context;
    this.reduceId = context.getReduceId();
    this.mrJobConf = context.getJobConf();
    this.umbilical = context.getUmbilical();
    this.reduceTask = context.getReduceTask();
    this.taskStatus = context.getStatus();
    this.rssJobConf = new JobConf(RssMRConfig.RSS_CONF_FILE);
    this.appId = RssMRUtils.getApplicationAttemptId().toString();
    this.appAttemptId = RssMRUtils.getApplicationAttemptId().getAttemptId();
    this.keyClass = context.getJobConf().getMapOutputKeyClass();
    this.valueClass = context.getJobConf().getMapOutputValueClass();
    this.rawComparator = context.getJobConf().getOutputKeyComparator();
    this.rssConf = RssMRConfig.toRssConf(rssJobConf);
    SerializerFactory factory = new SerializerFactory(this.rssConf);
    this.keySerializer = factory.getSerializer(keyClass).newInstance();
    this.valueSerializer = factory.getSerializer(valueClass).newInstance();
    this.partitionId = reduceId.getTaskID().getId();
    this.serverInfoSet = RssMRUtils.getAssignedServers(rssJobConf, partitionId);
    if (serverInfoSet.size() != 1) {
      throw new RssException("For now, only support one shuffle server.");
    }

    Combiner combiner = null;
    if (context.getCombinerClass() != null) {
      combiner = new MRCombiner<>(new JobConf(), context.getCombinerClass());
    }
    Map<Integer, List<ShuffleServerInfo>> serverInfoMap = new HashMap<>();
    serverInfoMap.put(partitionId, new ArrayList<>(serverInfoSet));
    this.reader = new RMRecordsReader(appId, 0, Sets.newHashSet(partitionId), serverInfoMap, this.rssConf, keyClass,
        valueClass, rawComparator, combiner, combiner != null, new MRMetricsReporter(context.getReporter()));
  }

  @Override
  public RawKeyValueIterator run() throws IOException, InterruptedException {
    reportUniqueBlockIds();
    taskStatus.setPhase(TaskStatus.Phase.SORT);
    reduceTask.statusUpdate(umbilical);
    reader.start();
    // When shuffle server is busy, reducer will wait for long time, and will not call increase counter to set
    // progress flag for long time. Then "Timed out" will be thrown. So we should set the flag bit periodically.
    this.scheduledExecutorService = ThreadUtils.getDaemonSingleThreadScheduledExecutor("PingThread");
    long interval = mrJobConf.getLong(MRJobConfig.TASK_TIMEOUT, MRJobConfig.DEFAULT_TASK_TIMEOUT_MILLIS) / 2;
    this.scheduledExecutorService.scheduleAtFixedRate(() -> context.getReporter().progress(), interval, interval,
        TimeUnit.MILLISECONDS);
    this.relayer = new RecordsRelayer(reader, keySerializer, valueSerializer);
    return relayer;
  }

  public void reportUniqueBlockIds() {
    ShuffleWriteClient writeClient = RssMRUtils.createShuffleClient(mrJobConf);
    Roaring64NavigableMap blockIdBitmap = writeClient.getShuffleResult(null, serverInfoSet, appId, 0, partitionId);

    final RssEventFetcher<K, V> eventFetcher = createEventFetcher();
    Roaring64NavigableMap taskIdBitmap = eventFetcher.fetchAllRssTaskIds();

    Roaring64NavigableMap uniqueBlockIdBitMap = Roaring64NavigableMap.bitmapOf();
    blockIdBitmap.forEach(blockId -> {
      long taId = RssMRUtils.getTaskAttemptId(blockId);
      if (taskIdBitmap.contains(taId)) {
        uniqueBlockIdBitMap.add(blockId);
      }
    });
    writeClient.reportUniqueBlocks(serverInfoSet, appId, 0, partitionId, uniqueBlockIdBitMap);
  }

  public static class RecordsRelayer implements RawKeyValueIterator {

    RMRecordsReader reader;
    KeyValueReader keyValueReader;
    SerializerInstance keySerializer;
    SerializerInstance valueSerializer;
    private Progress mergeProgress = new Progress();

    public RecordsRelayer(RMRecordsReader reader, SerializerInstance keySerializer,
                          SerializerInstance valueSerializer) {
      this.reader = reader;
      this.keyValueReader = this.reader.keyValueReader();
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;
    }

    @Override
    public DataInputBuffer getKey() throws IOException {
      // MapReduce api need to return raw key bytes. But if combine is required, need to real value object.
      // So need to serialized object.
      DataOutputBuffer buffer = new DataOutputBuffer();
      this.keySerializer.serialize(keyValueReader.getCurrentKey(), buffer);
      DataInputBuffer inputBuffer = new DataInputBuffer();
      inputBuffer.reset(buffer.getData(), 0, buffer.getLength());
      return inputBuffer;
    }

    @Override
    public DataInputBuffer getValue() throws IOException {
      // MapReduce api need to return raw value bytes. But if combine is required, need to real value object.
      // So need to serialized object.
      DataOutputBuffer buffer = new DataOutputBuffer();
      this.valueSerializer.serialize(keyValueReader.getCurrentValue(), buffer);
      DataInputBuffer inputBuffer = new DataInputBuffer();
      inputBuffer.reset(buffer.getData(), 0, buffer.getLength());
      return inputBuffer;
    }

    @Override
    public boolean next() throws IOException {
      return keyValueReader.next();
    }

    @Override
    public void close() throws IOException {
      this.reader.close();
    }

    @Override
    public Progress getProgress() {
      return mergeProgress;
    }
  }

  @Override
  public void close() {
    if (relayer != null) {
      try {
        relayer.close();
      } catch (IOException e) {
        throw new RssException(e);
      }
    }
  }

  @Override
  public void reportException(Throwable throwable) {
  }

  @VisibleForTesting
  void setReader(RMRecordsReader reader) {
    this.reader = reader;
  }

  @VisibleForTesting
  RssEventFetcher<K, V> createEventFetcher() {
    return new RssEventFetcher<K, V>(appAttemptId, reduceId, this.umbilical, mrJobConf, MAX_EVENTS_TO_FETCH);
  }
}
