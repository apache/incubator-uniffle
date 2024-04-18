package org.apache.spark.shuffle.writer;

import static org.apache.spark.shuffle.RssSparkConfig.RSS_CLIENT_SEND_SIZE_LIMITATION;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.shuffle.RssSparkConfig;
import org.apache.uniffle.client.shuffle.RecordBlob;
import org.apache.uniffle.client.shuffle.RecordCollection;
import org.apache.uniffle.client.shuffle.writer.Combiner;
import org.apache.uniffle.client.shuffle.RecordBuffer;
import org.apache.uniffle.client.shuffle.writer.WrappedByteArrayOutputStream;
import org.apache.uniffle.client.util.ClientUtils;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.records.RecordsWriter;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.ChecksumUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RMWriteBufferManager<K, V, C> extends MemoryConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(RMWriteBufferManager.class);

  private final int shuffleId;
  private final String taskId;
  private final long taskAttemptId;
  BufferManagerOptions options;
  private final Map<Integer, List<ShuffleServerInfo>> partitionToServers;
  private final int bufferSize;
  private final long spillSize;
  private final long askExecutorMemory;
  private final long requireMemoryInterval;
  private final int requireMemoryRetryMax;
  private final RssConf rssConf;
  private Comparator comparator;
  private final Combiner combiner;
  private final Class<K> keyClass;
  private final Class<V> valueClass;
  private final Class<C> combineClass;
  private final boolean mapSideCombine;

  private BlockIdLayout blockIdLayout;
  private Map<Integer, Integer> partitionToSeqNo = new HashMap<>();
  private Map<Integer, RecordBuffer<K, V>> buffers = new HashMap<>();
  private long sendSizeLimit;

  private boolean memorySpillEnabled;
  private int memorySpillTimeoutSec;
  private Function<List<ShuffleBlockInfo>, List<CompletableFuture<Long>>> spillFunc;

  private final MemoryUsage memoryUsage;

  public RMWriteBufferManager(
      int shuffleId,
      String taskId,
      long taskAttemptId,
      BufferManagerOptions options,
      Map<Integer, List<ShuffleServerInfo>> partitionToServers,
      TaskMemoryManager taskMemoryManager,
      RssConf rssConf,
      Comparator comparator,
      Combiner combiner,
      Class<K> keyClass,
      Class<V> valueClass,
      Class<C> combinerClass,
      boolean mapSideCombine,
      int maxRecordsPerBuffer,
      int maxRecords,
      Function<List<ShuffleBlockInfo>, List<CompletableFuture<Long>>> spillFunc) {
    super(taskMemoryManager, taskMemoryManager.pageSizeBytes(), MemoryMode.ON_HEAP);
    this.shuffleId = shuffleId;
    this.taskId = taskId;
    this.taskAttemptId = taskAttemptId;
    this.options = options;
    this.partitionToServers = partitionToServers;
    this.bufferSize = options.getBufferSize();
    this.spillSize = options.getBufferSpillThreshold();
    this.askExecutorMemory = options.getPreAllocatedBufferSize();
    this.requireMemoryInterval = options.getRequireMemoryInterval();
    this.requireMemoryRetryMax = options.getRequireMemoryRetryMax();
    this.rssConf = rssConf;
    this.blockIdLayout = BlockIdLayout.from(rssConf);
    this.sendSizeLimit = rssConf.getLong(RSS_CLIENT_SEND_SIZE_LIMITATION);
    this.comparator = comparator;
    if (this.comparator == null) {
      this.comparator = new Comparator() {
        @Override
        public int compare(Object o1, Object o2) {
          int h1 = (o1 == null) ? 0 : o1.hashCode();
          int h2 = (o2 == null) ? 0 : o2.hashCode();
          return h1 < h2 ? -1 : h1 == h2 ? 0 : 1;
        }
      };
    }
    this.combiner = combiner;
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.combineClass = combinerClass;
    this.mapSideCombine = mapSideCombine;
    this.memoryUsage = new MemoryUsage(this.bufferSize, this.spillSize, maxRecords, maxRecordsPerBuffer);
    this.memorySpillEnabled = rssConf.get(RssSparkConfig.RSS_MEMORY_SPILL_ENABLED);
    this.memorySpillTimeoutSec = rssConf.get(RssSparkConfig.RSS_MEMORY_SPILL_TIMEOUT);
    this.spillFunc = spillFunc;
  }

  public List<ShuffleBlockInfo> addRecord(int partitionId, K key, V value){
    memoryUsage.waitForMemory();
    memoryUsage.incrNumRecordInBuffer();
    List<ShuffleBlockInfo> candidateSendingBlocks = insertIntoBuffer(partitionId, key, value);
    if (memoryUsage.shouldSendBuffer()) {
      candidateSendingBlocks.addAll(clear());
    }
    return candidateSendingBlocks;
  }

  private List<ShuffleBlockInfo> insertIntoBuffer(int partitionId, K key, V value) {
    List<ShuffleBlockInfo> sentBlocks = new ArrayList<>();
    if (buffers.containsKey(partitionId)) {
      RecordBuffer<K, V> wb = buffers.get(partitionId);
      wb.addRecord(key, value);
      if (memoryUsage.overMemorySizePerBuffer(wb.size())) {
        sentBlocks.add(createShuffleBlock(partitionId, wb));
        buffers.remove(partitionId);
      }
    } else {
      RecordBuffer<K, V> wb = new RecordBuffer(partitionId);
      wb.addRecord(key, value);
      buffers.put(partitionId, wb);
    }
    return sentBlocks;
  }

  // transform records to shuffleBlock
  protected ShuffleBlockInfo createShuffleBlock(int partitionId, RecordBuffer<K, V> rb) {
    // In general, when we use RMWriteBufferManager, we need either sort or combine.
    // For case that only need combine and don't need sorting, we first sort by hashcode then combine.
    // So we sort firstly, then choose whether to combine.
    try {
      RecordCollection rc = rb;
      rb.sort(comparator);
      if (mapSideCombine) {
        RecordBlob<K, V, C> blob = new RecordBlob<>(partitionId);
        blob.addRecords(rb);
        blob.combine(combiner, false);
        rc = blob;
      }
      WrappedByteArrayOutputStream outputStream = new WrappedByteArrayOutputStream(rb.size() * 2);
      RecordsWriter<K, ?> writer =
          new RecordsWriter(rssConf, outputStream, keyClass, combineClass != null ? combineClass : valueClass);
      rc.serialize(writer);
      writer.close();
      final long crc32 = ChecksumUtils.getCrc32(outputStream.getBuf());
      final long blockId =
          blockIdLayout.getBlockId(getNextSeqNo(partitionId), partitionId, taskAttemptId);
      int length = outputStream.size();
      byte[] buffer = new byte[length];
      System.arraycopy(outputStream.getBuf(), 0, buffer, 0, length);
      memoryUsage.setUpForSend(rb.size(), length);
      if (rc.size() > 0) {
        int estimated = length / rc.size();
        memoryUsage.updateEstimatedRecordSize(estimated);
      }
      return new ShuffleBlockInfo(
          shuffleId,
          partitionId,
          blockId,
          length,
          crc32,
          buffer,
          partitionToServers.get(partitionId),
          length,
          length,
          taskAttemptId);
    } catch (IOException e) {
      throw new RssException(e);
    }
  }

  // transform all [partition, records] to [partition, ShuffleBlockInfo] and clear cache
  public synchronized List<ShuffleBlockInfo> clear() {
    List<ShuffleBlockInfo> result = new ArrayList<>();
    Iterator<Map.Entry<Integer, RecordBuffer<K, V>>> iterator = buffers.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Integer, RecordBuffer<K, V>> entry = iterator.next();
      RecordBuffer<K, V> wb = entry.getValue();
      result.add(createShuffleBlock(entry.getKey(), wb));
      iterator.remove();
    }
    return result;
  }

  // it's run in single thread, and is not thread safe
  private int getNextSeqNo(int partitionId) {
    partitionToSeqNo.putIfAbsent(partitionId, 0);
    int seqNo = partitionToSeqNo.get(partitionId);
    partitionToSeqNo.put(partitionId, seqNo + 1);
    return seqNo;
  }

  public List<AddBlockEvent> buildBlockEvents(List<ShuffleBlockInfo> shuffleBlockInfoList) {
    long totalSize = 0;
    long memoryUsed = 0;
    List<AddBlockEvent> events = new ArrayList<>();
    List<ShuffleBlockInfo> shuffleBlockInfosPerEvent = new ArrayList<>();
    for (ShuffleBlockInfo sbi : shuffleBlockInfoList) {
      totalSize += sbi.getSize();
      memoryUsed += sbi.getFreeMemory();
      shuffleBlockInfosPerEvent.add(sbi);
      // split shuffle data according to the size
      if (totalSize > sendSizeLimit) {
        LOG.debug(
            "Build event with "
                + shuffleBlockInfosPerEvent.size()
                + " blocks and "
                + totalSize
                + " bytes");
        final long memoryUsedTemp = memoryUsed;
        final List<ShuffleBlockInfo> shuffleBlocksTemp = shuffleBlockInfosPerEvent;
        events.add(
            new AddBlockEvent(
                taskId,
                shuffleBlockInfosPerEvent,
                () -> {
                  memoryUsage.freeAllocatedMemory(memoryUsedTemp);
                  shuffleBlocksTemp.stream().forEach(x -> x.getData().release());
                }));
        shuffleBlockInfosPerEvent = new ArrayList<>();
        totalSize = 0;
        memoryUsed = 0;
      }
    }
    if (!shuffleBlockInfosPerEvent.isEmpty()) {
      LOG.debug(
          "Build event with "
              + shuffleBlockInfosPerEvent.size()
              + " blocks and "
              + totalSize
              + " bytes");
      final List<ShuffleBlockInfo> shuffleBlocksTemp = shuffleBlockInfosPerEvent;
      final long memoryUsedTemp = memoryUsed;
      events.add(
          new AddBlockEvent(
              taskId,
              shuffleBlockInfosPerEvent,
              () -> {
                memoryUsage.freeAllocatedMemory(memoryUsedTemp);
                shuffleBlocksTemp.stream().forEach(x -> x.getData().release());
              }));
    }
    return events;
  }

  @Override
  public long spill(long size, MemoryConsumer trigger) throws IOException {
    if (!memorySpillEnabled || trigger != this) {
      return 0L;
    }

    List<CompletableFuture<Long>> futures = spillFunc.apply(clear());
    CompletableFuture<Void> allOfFutures =
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
    try {
      allOfFutures.get(memorySpillTimeoutSec, TimeUnit.SECONDS);
    } catch (TimeoutException timeoutException) {
      // A best effort strategy to wait.
      // If timeout exception occurs, the underlying tasks won't be cancelled.
    } finally {
      long releasedSize =
          futures.stream()
              .filter(x -> x.isDone())
              .mapToLong(
                  x -> {
                    try {
                      return x.get();
                    } catch (Exception e) {
                      return 0;
                    }
                  })
              .sum();
      LOG.info(
          "[taskId: {}] Spill triggered by own, released memory size: {}", taskId, releasedSize);
      return releasedSize;
    }
  }

  private class MemoryUsage {

    private long allocatedBytes;
    private final long maxBufferSize;
    private final long spillSize;
    private final int maxRecords;
    private final int maxRecordsPerBuffer;

    private long numRecordInBuffer = 0;
    private long inSendListBytes = 0;
    private double estimatedRecordSize = -1.0;

    private MemoryUsage(long maxBufferSize, long spillSize,
                        int maxRecords, int maxRecordsPerBuffer) {
      this.maxBufferSize = maxBufferSize;
      this.spillSize = spillSize;
      this.maxRecords = maxRecords;
      this.maxRecordsPerBuffer = maxRecordsPerBuffer;
    }

    synchronized void incrNumRecordInBuffer() {
      numRecordInBuffer++;
    }

    synchronized void setUpForSend(int recordSize, long serializedBytes) {
      numRecordInBuffer -= recordSize;
      inSendListBytes += serializedBytes;
      notifyAll();
    }

    synchronized void updateEstimatedRecordSize(int estimated) {
      estimatedRecordSize = estimatedRecordSize > 0 ? (int) (estimated * 0.6 + estimatedRecordSize * 0.4) : estimated;
      notifyAll();
    }

    synchronized boolean overMemorySizePerBuffer(int records) {
      if (estimatedRecordSize > 0) {
        return (records * estimatedRecordSize) > maxBufferSize;
      } else {
        return records > maxRecordsPerBuffer;
      }
    }

    synchronized boolean shouldSendBuffer() {
      if (estimatedRecordSize > 0) {
        return (numRecordInBuffer * estimatedRecordSize) > spillSize;
      } else {
        return numRecordInBuffer > maxRecords;
      }
    }

    synchronized void waitForMemory() {
      if (estimatedRecordSize > 0) {
        requestMemory((long) estimatedRecordSize);
      }
    }

    synchronized long getUsedBytes() {
      return (long) (estimatedRecordSize * numRecordInBuffer + inSendListBytes);
    }

    synchronized void requestMemory(long requiredMem) {
      if (allocatedBytes - getUsedBytes() > requiredMem) {
        return;
      }
      long gotMem = acquireMemory(askExecutorMemory);
      allocatedBytes += gotMem;
      int retry = 0;
      while (allocatedBytes - getUsedBytes() < requiredMem) {
        LOG.info(
            "Can't get memory for now, sleep and try["
                + retry
                + "] again, request["
                + askExecutorMemory
                + "], got["
                + gotMem
                + "] less than "
                + requiredMem);
        try {
          wait(requireMemoryInterval);
        } catch (InterruptedException ie) {
          LOG.warn("Exception happened when waiting for memory.", ie);
        }
        gotMem = acquireMemory(askExecutorMemory);
        allocatedBytes += gotMem;
        retry++;
        if (retry > requireMemoryRetryMax) {
          String message =
              "Can't get memory to cache shuffle data, request["
                  + askExecutorMemory
                  + "], got["
                  + gotMem
                  + "],"
                  + " WriteBufferManager allocated["
                  + allocatedBytes
                  + "] task used["
                  + used
                  + "]. It may be caused by shuffle server is full of data"
                  + " or consider to optimize 'spark.executor.memory',"
                  + " 'spark.rss.writer.buffer.spill.size'.";
          LOG.error(message);
          throw new RssException(message);
        }
      }
    }

    synchronized void freeAllocatedMemory(long freeMemory) {
      freeMemory(freeMemory);
      allocatedBytes -= freeMemory;
      inSendListBytes -= freeMemory;
    }

    @Override
    public String toString() {
      return "MemoryUsage{" +
          "allocatedBytes=" + allocatedBytes +
          ", maxBufferSize=" + maxBufferSize +
          ", spillSize=" + spillSize +
          ", maxRecords=" + maxRecords +
          ", maxRecordsPerBuffer=" + maxRecordsPerBuffer +
          ", numRecordInBuffer=" + numRecordInBuffer +
          ", inSendListBytes=" + inSendListBytes +
          ", estimatedRecordSize=" + estimatedRecordSize +
          '}';
    }
  }
}
