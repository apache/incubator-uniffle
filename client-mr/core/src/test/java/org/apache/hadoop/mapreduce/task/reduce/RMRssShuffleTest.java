package org.apache.hadoop.mapreduce.task.reduce;

import static org.apache.uniffle.common.serializer.SerializerUtils.genData;
import static org.apache.uniffle.common.serializer.SerializerUtils.genSortedRecordBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.ShuffleConsumerPlugin;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.RssMRUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.uniffle.client.api.ShuffleServerClient;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.shuffle.reader.MockedShuffleServerClient;
import org.apache.uniffle.client.shuffle.reader.MockedShuffleWriteClient;
import org.apache.uniffle.client.shuffle.reader.RMRecordsReader;
import org.apache.uniffle.client.shuffle.writer.SumByKeyCombiner;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.merger.AbstractSegment;
import org.apache.uniffle.common.merger.Merger;
import org.apache.uniffle.common.serializer.Serializer;
import org.apache.uniffle.common.serializer.SerializerFactory;
import org.apache.uniffle.common.serializer.SerializerInstance;
import org.apache.uniffle.common.serializer.SerializerUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class RMRssShuffleTest {

  private final static ApplicationAttemptId APPLICATION_ATTEMPT_ID =
      ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 0), 0);
  private final static String APP_ID = APPLICATION_ATTEMPT_ID.toString();
  private final static int SHUFFLE_ID = 0;
  private final static int PARTITION_ID = 0;
  private final static int RECORDS_NUM = 1009;

  @Test
  @Timeout(10)
  public void testReadShuffleWithoutCombine() throws Exception {
    // 1 basic parameter
    final Class keyClass = Text.class;
    final Class valueClass = IntWritable.class;
    final RawComparator comparator = new Text.Comparator();
    final SumByKeyCombiner combiner = null;
    final RssConf rssConf = new RssConf();
    final List<ShuffleServerInfo> serverInfos = Lists.newArrayList(new ShuffleServerInfo("dummy", -1));
    final long taskAttemptId = 0;
    final long[] blockIds = new long[] {RssMRUtils.getBlockId(PARTITION_ID, taskAttemptId, 0)};
    final JobConf jobConf = new JobConf();
    jobConf.setMapOutputKeyClass(keyClass);
    jobConf.setMapOutputValueClass(valueClass);
    jobConf.setOutputKeyComparatorClass(comparator.getClass());

    try (MockedStatic<RssMRUtils> utils = Mockito.mockStatic(RssMRUtils.class)) {
      // 2 mock for RssMRUtils
      ShuffleWriteClient writeClient = new MockedShuffleWriteClient();
      Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds = new HashMap();
      serverToPartitionToBlockIds.put(serverInfos.get(0), ImmutableMap.of(PARTITION_ID, Sets.newHashSet(blockIds[0])));
      writeClient.reportShuffleResult(serverToPartitionToBlockIds, APP_ID, SHUFFLE_ID, 0, 0);
      utils.when(() -> RssMRUtils.createShuffleClient(any())).thenReturn(writeClient);
      utils.when(() -> RssMRUtils.getApplicationAttemptId()).thenReturn(APPLICATION_ATTEMPT_ID);
      utils.when(() -> RssMRUtils.getAssignedServers(any(), anyInt())).thenReturn(new HashSet<>(serverInfos));

      // 3 construct shuffle plugin context
      final ShuffleConsumerPlugin.Context context = mock(ShuffleConsumerPlugin.Context.class);
      when(context.getJobConf()).thenReturn(jobConf);
      when(context.getReduceId()).thenReturn(new TaskAttemptID());
      when(context.getCombinerClass()).thenReturn(null);
      when(context.getStatus()).thenReturn(mock(TaskStatus.class));
      when(context.getReduceTask()).thenReturn(mock(Task.class));

      // 4 create reader
      RMRecordsReader reader = new RMRecordsReader(APP_ID, SHUFFLE_ID, Sets.newHashSet(PARTITION_ID),
          ImmutableMap.of(PARTITION_ID, serverInfos), rssConf, keyClass, valueClass, comparator, combiner, false, null);
      RMRecordsReader readerSpy = spy(reader);
      ByteBuffer byteBuffer = ByteBuffer.wrap(genSortedRecordBytes(rssConf, keyClass, valueClass, 0, 1, RECORDS_NUM, 1));
      ShuffleServerClient serverClient = new MockedShuffleServerClient(new int[] {PARTITION_ID},
          new ByteBuffer[][] {{byteBuffer}}, blockIds);
      doReturn(serverClient).when(readerSpy).createShuffleServerClient(any());

      // 5 create and init rss shuffle, mock RssEventFetcher
      RMRssShuffle shuffle = new RMRssShuffle();
      RMRssShuffle shuffleSpy = spy(shuffle);
      RssEventFetcher eventFetcher = mock(RssEventFetcher.class);
      when(eventFetcher.fetchAllRssTaskIds()).thenReturn(Roaring64NavigableMap.bitmapOf(taskAttemptId));
      doReturn(eventFetcher).when(shuffleSpy).createEventFetcher();
      shuffleSpy.init(context);
      shuffleSpy.setReader(readerSpy);

      // 6 run rss shuffle and verify result
      RawKeyValueIterator iterator = shuffleSpy.run();
      SerializerFactory factory = new SerializerFactory(rssConf);
      Serializer serializer = factory.getSerializer(Text.class);
      SerializerInstance instance = serializer.newInstance();
      int index = 0;
      while (iterator.next()) {
        DataInputBuffer dataInputBuffer = iterator.getKey();
        assertEquals(genData(Text.class, index), instance.deserialize(dataInputBuffer, keyClass));
        dataInputBuffer = iterator.getValue();
        assertEquals(genData(IntWritable.class, index), instance.deserialize(dataInputBuffer, valueClass));
        index++;
      }
      assertEquals(RECORDS_NUM, index);
    }
  }

  @Test
  @Timeout(10)
  public void testReadShuffleWithCombine() throws Exception {
    // 1 basic parameter
    final Class keyClass = Text.class;
    final Class valueClass = IntWritable.class;
    final RawComparator comparator = new Text.Comparator();
    final SumByKeyCombiner combiner = new SumByKeyCombiner();
    final RssConf rssConf = new RssConf();
    final List<ShuffleServerInfo> serverInfos = Lists.newArrayList(new ShuffleServerInfo("dummy", -1));
    final long taskAttemptId = 0;
    final long[] blockIds = new long[] {RssMRUtils.getBlockId(PARTITION_ID, taskAttemptId, 0)};
    final JobConf jobConf = new JobConf();
    jobConf.setMapOutputKeyClass(keyClass);
    jobConf.setMapOutputValueClass(valueClass);
    jobConf.setOutputKeyComparatorClass(comparator.getClass());

    try (MockedStatic<RssMRUtils> utils = Mockito.mockStatic(RssMRUtils.class)) {
      // 2 mock for RssMRUtils
      ShuffleWriteClient writeClient = new MockedShuffleWriteClient();
      Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds = new HashMap();
      serverToPartitionToBlockIds.put(serverInfos.get(0), ImmutableMap.of(PARTITION_ID, Sets.newHashSet(blockIds[0])));
      writeClient.reportShuffleResult(serverToPartitionToBlockIds, APP_ID, SHUFFLE_ID, 0, 0);
      utils.when(() -> RssMRUtils.createShuffleClient(any())).thenReturn(writeClient);
      utils.when(() -> RssMRUtils.getApplicationAttemptId()).thenReturn(APPLICATION_ATTEMPT_ID);
      utils.when(() -> RssMRUtils.getAssignedServers(any(), anyInt())).thenReturn(new HashSet<>(serverInfos));

      // 3 construct shuffle plugin context
      final ShuffleConsumerPlugin.Context context = mock(ShuffleConsumerPlugin.Context.class);
      when(context.getJobConf()).thenReturn(jobConf);
      when(context.getReduceId()).thenReturn(new TaskAttemptID());
      when(context.getCombinerClass()).thenReturn(combiner.getClass());
      when(context.getStatus()).thenReturn(mock(TaskStatus.class));
      when(context.getReduceTask()).thenReturn(mock(Task.class));

      // 4 create reader
      RMRecordsReader reader = new RMRecordsReader(APP_ID, SHUFFLE_ID, Sets.newHashSet(PARTITION_ID),
          ImmutableMap.of(PARTITION_ID, serverInfos), rssConf, keyClass, valueClass, comparator, combiner, false, null);
      RMRecordsReader readerSpy = spy(reader);

      List<AbstractSegment> segments = new ArrayList<>();
      segments.add(SerializerUtils.genMemorySegment(rssConf, keyClass, valueClass, 0L, 0, 2, RECORDS_NUM));
      segments.add(SerializerUtils.genMemorySegment(rssConf, keyClass, valueClass, 1L, 0, 2, RECORDS_NUM));
      segments.add(SerializerUtils.genMemorySegment(rssConf, keyClass, valueClass, 2L, 1, 2, RECORDS_NUM));
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      Merger.merge(rssConf, output, segments, keyClass, valueClass, comparator);
      output.close();
      ByteBuffer byteBuffer = ByteBuffer.wrap(output.toByteArray());
      ShuffleServerClient serverClient = new MockedShuffleServerClient(new int[] {PARTITION_ID},
          new ByteBuffer[][] {{byteBuffer}}, blockIds);
      doReturn(serverClient).when(readerSpy).createShuffleServerClient(any());

      // 5 create and init rss shuffle, mock RssEventFetcher
      RMRssShuffle shuffle = new RMRssShuffle();
      RMRssShuffle shuffleSpy = spy(shuffle);
      RssEventFetcher eventFetcher = mock(RssEventFetcher.class);
      when(eventFetcher.fetchAllRssTaskIds()).thenReturn(Roaring64NavigableMap.bitmapOf(taskAttemptId));
      doReturn(eventFetcher).when(shuffleSpy).createEventFetcher();
      shuffleSpy.init(context);
      shuffleSpy.setReader(readerSpy);

      // 6 run rss shuffle and verify result
      RawKeyValueIterator iterator = shuffleSpy.run();
      SerializerFactory factory = new SerializerFactory(rssConf);
      Serializer serializer = factory.getSerializer(Text.class);
      SerializerInstance instance = serializer.newInstance();
      int index = 0;
      while (iterator.next()) {
        DataInputBuffer dataInputBuffer = iterator.getKey();
        assertEquals(genData(Text.class, index), instance.deserialize(dataInputBuffer, keyClass));
        dataInputBuffer = iterator.getValue();
        Object value = SerializerUtils.genData(valueClass, index);
        Object newValue = value;
        if (index % 2 == 0) {
          if (value instanceof IntWritable) {
            newValue = new IntWritable(((IntWritable) value).get() * 2);
          } else {
            newValue = (int) value * 2;
          }
        }
        assertEquals(newValue, instance.deserialize(dataInputBuffer, valueClass));
        index++;
      }
      assertEquals(RECORDS_NUM * 2, index);
    }
  }
}
