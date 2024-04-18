package org.apache.tez.runtime.library.common.sort.buffer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.io.RawComparator;
import org.apache.tez.common.RssTezUtils;
import org.apache.tez.common.counters.TezCounter;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.response.SendShuffleDataResult;
import org.apache.uniffle.client.shuffle.RecordBuffer;
import org.apache.uniffle.client.shuffle.writer.WrappedByteArrayOutputStream;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.records.RecordsWriter;
import org.apache.uniffle.common.util.ChecksumUtils;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RMWriteBufferManager<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(RMWriteBufferManager.class);

  private final Map<Integer, RecordBuffer<K, V>> buffers = JavaUtils.newConcurrentMap();
  private final Map<Integer, Integer> partitionToSeqNo = Maps.newHashMap();
  private final long taskAttemptId;
  private final int shuffleId;
  private final int batch;
  private final Map<Integer, List<ShuffleServerInfo>> partitionToServers;
  private final ReentrantLock memoryLock = new ReentrantLock();
  private final Condition full = memoryLock.newCondition();
  private final Comparator<K> comparator;
  private final Set<Long> successBlockIds;
  private final Set<Long> failedBlockIds;
  private final List<RecordBuffer<K, V>> waitSendBuffers = Lists.newLinkedList();
  private final String appId;
  private final ShuffleWriteClient shuffleWriteClient;
  private final long sendCheckTimeout;
  private final long sendCheckInterval;
  private final Set<Long> allBlockIds = Sets.newConcurrentHashSet();
  private final int bitmapSplitNum;
  // server -> partitionId -> blockIds
  private Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds =
      Maps.newConcurrentMap();
  private final boolean isMemoryShuffleEnabled;
  private final int numMaps;
  private final ExecutorService sendExecutorService;
  private final RssConf rssConf;
  private Class<K> keyClass;
  private Class<V> valueClass;

  private final TezCounter mapOutputByteCounter;
  private final TezCounter mapOutputRecordCounter;

  private MemoryUsage memoryUsage;

  public RMWriteBufferManager(
      long maxMemSize,
      long taskAttemptId,
      int shuffleId,
      int batch,
      RawComparator<K> comparator,
      double memoryThreshold,
      String appId,
      ShuffleWriteClient shuffleWriteClient,
      long sendCheckInterval,
      long sendCheckTimeout,
      Map<Integer, List<ShuffleServerInfo>> partitionToServers,
      Set<Long> successBlockIds,
      Set<Long> failedBlockIds,
      int bitmapSplitNum,
      int numMaps,
      boolean isMemoryShuffleEnabled,
      int sendThreadNum,
      double sendThreshold,
      RssConf rssConf,
      Class<K> keyClass,
      Class<V> valueClass,
      TezCounter mapOutputByteCounter,
      TezCounter mapOutputRecordCounter,
      long maxBufferSize,
      int maxRecordsPerBuffer,
      int maxRecords) {
    this.taskAttemptId = taskAttemptId;
    this.shuffleId = shuffleId;
    this.batch = batch;
    this.comparator = comparator;
    this.appId = appId;
    this.shuffleWriteClient = shuffleWriteClient;
    this.sendCheckInterval = sendCheckInterval;
    this.sendCheckTimeout = sendCheckTimeout;
    this.partitionToServers = partitionToServers;
    this.successBlockIds = successBlockIds;
    this.failedBlockIds = failedBlockIds;
    this.bitmapSplitNum = bitmapSplitNum;
    this.numMaps = numMaps;
    this.isMemoryShuffleEnabled = isMemoryShuffleEnabled;
    this.sendExecutorService = ThreadUtils.getDaemonFixedThreadPool(sendThreadNum, "send-thread");
    this.rssConf = rssConf;
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.mapOutputByteCounter = mapOutputByteCounter;
    this.mapOutputRecordCounter = mapOutputRecordCounter;
    this.memoryUsage = new MemoryUsage(maxMemSize, memoryThreshold, sendThreshold, maxBufferSize, maxRecords,
        maxRecordsPerBuffer);
  }

  // todo: Single Buffer should also have its size limit
  public void addRecord(int partitionId, K key, V value) throws IOException, InterruptedException {
    // Wait util memory is not busy
    memoryUsage.waitForMemory();

    // Fail fast if there are some failed blocks.
    checkFailedBlocks();

    buffers.computeIfAbsent(
        partitionId,
        k -> {
          RecordBuffer<K, V> recordBuffer = new RecordBuffer(partitionId, true);
          waitSendBuffers.add(recordBuffer);
          return recordBuffer;
        });

    RecordBuffer<K, V> buffer = buffers.get(partitionId);
    buffer.addRecord(key, value);
    memoryUsage.incrNumRecordInBuffer();
    if (memoryUsage.overMemorySizePerBuffer(buffer.size())) {
      if (waitSendBuffers.remove(buffer)) {
        sendBufferToServers(buffer);
      } else {
        LOG.error("waitSendBuffers don't contain buffer {}", buffer);
      }
    }
    if (memoryUsage.shouldSendBuffer()) {
      sendBuffersToServers();
    }
  }

  private void sendBufferToServers(RecordBuffer<K, V> buffer) {
    List<ShuffleBlockInfo> shuffleBlocks = Lists.newArrayList();
    prepareBufferForSend(shuffleBlocks, buffer);
    sendShuffleBlocks(shuffleBlocks);
  }

  void sendBuffersToServers() {
    // Send the partition that have more records firstly
    waitSendBuffers.sort(
        new Comparator<RecordBuffer<K, V>>() {
          @Override
          public int compare(RecordBuffer<K, V> o1, RecordBuffer<K, V> o2) {
            return o2.size() - o1.size();
          }
        });
    int sendSize = Math.min(batch, waitSendBuffers.size());
    Iterator<RecordBuffer<K, V>> iterator = waitSendBuffers.iterator();
    int index = 0;
    List<ShuffleBlockInfo> shuffleBlocks = Lists.newArrayList();
    while (iterator.hasNext() && index < sendSize) {
      RecordBuffer<K, V> buffer = iterator.next();
      prepareBufferForSend(shuffleBlocks, buffer);
      iterator.remove();
      index++;
    }
    sendShuffleBlocks(shuffleBlocks);
  }

  private void prepareBufferForSend(List<ShuffleBlockInfo> shuffleBlocks, RecordBuffer<K, V> buffer) {
    buffers.remove(buffer.getPartitionId());
    ShuffleBlockInfo block = createShuffleBlock(buffer);
    buffer.clear();
    shuffleBlocks.add(block);
    allBlockIds.add(block.getBlockId());
    block
        .getShuffleServerInfos()
        .forEach(
            shuffleServerInfo -> {
              Map<Integer, Set<Long>> pToBlockIds =
                  serverToPartitionToBlockIds.computeIfAbsent(
                      shuffleServerInfo, k -> Maps.newHashMap());
              pToBlockIds
                  .computeIfAbsent(block.getPartitionId(), v -> Sets.newHashSet())
                  .add(block.getBlockId());
            });
  }

  private void sendShuffleBlocks(List<ShuffleBlockInfo> shuffleBlocks) {
    sendExecutorService.submit(
        new Runnable() {
          @Override
          public void run() {
            long size = 0;
            try {
              for (ShuffleBlockInfo block : shuffleBlocks) {
                size += block.getFreeMemory();
              }
              SendShuffleDataResult result =
                  shuffleWriteClient.sendShuffleData(appId, shuffleBlocks, () -> false);
              successBlockIds.addAll(result.getSuccessBlockIds());
              failedBlockIds.addAll(result.getFailedBlockIds());
            } catch (Throwable t) {
              LOG.warn("send shuffle data exception ", t);
            } finally {
              memoryUsage.descInSendListBytes(size);
            }
          }
        });
  }

  public void waitSendFinished() {
    while (!waitSendBuffers.isEmpty()) {
      sendBuffersToServers();
    }
    long start = System.currentTimeMillis();
    while (true) {
      checkFailedBlocks();
      // remove blockIds which was sent successfully, if there has none left, all data are sent
      allBlockIds.removeAll(successBlockIds);
      if (allBlockIds.isEmpty()) {
        break;
      }
      LOG.info("Wait " + allBlockIds.size() + " blocks sent to shuffle server");
      Uninterruptibles.sleepUninterruptibly(sendCheckInterval, TimeUnit.MILLISECONDS);
      if (System.currentTimeMillis() - start > sendCheckTimeout) {
        String errorMsg =
            "Timeout: failed because "
                + allBlockIds.size()
                + " blocks can't be sent to shuffle server in "
                + sendCheckTimeout
                + " ms.";
        LOG.error(errorMsg);
        throw new RssException(errorMsg);
      }
    }
    if (!isMemoryShuffleEnabled) {
      long s = System.currentTimeMillis();
      sendCommit();
    }

    start = System.currentTimeMillis();
    shuffleWriteClient.reportShuffleResult(
        serverToPartitionToBlockIds, appId, shuffleId, taskAttemptId, bitmapSplitNum);
    LOG.info(
        "Report shuffle result for task[{}] with bitmapNum[{}] cost {} ms",
        taskAttemptId,
        bitmapSplitNum,
        (System.currentTimeMillis() - start));
  }

  // if failed when send data to shuffle server, mark task as failed and throw exception.
  private void checkFailedBlocks() {
    if (failedBlockIds.size() > 0) {
      String errorMsg =
          "Send failed: failed because "
              + failedBlockIds.size()
              + " blocks can't be sent to shuffle server.";
      LOG.error(errorMsg);
      throw new RssException(errorMsg);
    }
  }

  // transform records to shuffleBlock
  ShuffleBlockInfo createShuffleBlock(RecordBuffer<K, V> rb) {
    try {
      rb.sort(comparator);
      WrappedByteArrayOutputStream outputStream = new WrappedByteArrayOutputStream(rb.size() * 2);
      RecordsWriter<K, V> writer = new RecordsWriter(rssConf, outputStream, keyClass, valueClass);
      rb.serialize(writer);
      writer.close();
      int partitionId = rb.getPartitionId();
      final long crc32 = ChecksumUtils.getCrc32(outputStream.getBuf());
      final long blockId = RssTezUtils.getBlockId(partitionId, taskAttemptId, getNextSeqNo(partitionId));
      int length = outputStream.size();
      byte[] buffer = new byte[length];
      System.arraycopy(outputStream.getBuf(), 0, buffer, 0, length);

      memoryUsage.setUpForSend(rb.size(), length);
      if (rb.size() > 0) {
        int estimated = length / rb.size();
        memoryUsage.updateEstimatedRecordSize(estimated);
      }
      mapOutputByteCounter.increment(length);
      mapOutputRecordCounter.increment(rb.size());
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
      throw new RuntimeException(e);
    }
  }

  protected void sendCommit() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Set<ShuffleServerInfo> serverInfos = Sets.newHashSet();
    for (List<ShuffleServerInfo> serverInfoLists : partitionToServers.values()) {
      for (ShuffleServerInfo serverInfo : serverInfoLists) {
        serverInfos.add(serverInfo);
      }
    }
    Future<Boolean> future =
        executor.submit(() -> shuffleWriteClient.sendCommit(serverInfos, appId, shuffleId, numMaps));
    long start = System.currentTimeMillis();
    int currentWait = 200;
    int maxWait = 5000;
    while (!future.isDone()) {
      LOG.info(
          "Wait commit to shuffle server for task["
              + taskAttemptId
              + "] cost "
              + (System.currentTimeMillis() - start)
              + " ms");
      Uninterruptibles.sleepUninterruptibly(currentWait, TimeUnit.MILLISECONDS);
      currentWait = Math.min(currentWait * 2, maxWait);
    }
    try {
      // check if commit/finish rpc is successful
      if (!future.get()) {
        throw new RssException("Failed to commit task to shuffle server");
      }
    } catch (InterruptedException ie) {
      LOG.warn("Ignore the InterruptedException which should be caused by internal killed");
    } catch (Exception e) {
      throw new RssException("Exception happened when get commit status", e);
    } finally {
      executor.shutdown();
    }
  }

  // it's run in single thread, and is not thread safe
  private int getNextSeqNo(int partitionId) {
    partitionToSeqNo.computeIfAbsent(partitionId, key -> 0);
    int seqNo = partitionToSeqNo.get(partitionId);
    partitionToSeqNo.put(partitionId, seqNo + 1);
    return seqNo;
  }

  public void freeAllResources() {
    sendExecutorService.shutdownNow();
  }

  @VisibleForTesting
  long getInSendListBytes() {
    return memoryUsage.getInSendListBytes();
  }

  @VisibleForTesting
  long getNumRecordInBuffer() {
    return memoryUsage.getNumRecordInBuffer();
  }

  private class MemoryUsage {
    private final long maxMemSize;
    private final double memoryThreshold;
    private final double maxMemoryForSend;
    private final long maxBufferSize;
    private final int maxRecords;
    private final int maxRecordsPerBuffer;

    private long numRecordInBuffer = 0;
    private long inSendListBytes = 0;
    private double estimatedRecordSize = -1.0;

    private MemoryUsage(long maxMemSize, double memoryThreshold, double sendThreshold, long maxBufferSize,
                        int maxRecords, int maxRecordsPerBuffer) {
      this.maxMemSize = maxMemSize;
      this.memoryThreshold = memoryThreshold;
      this.maxMemoryForSend = maxMemSize * sendThreshold;;
      this.maxBufferSize = maxBufferSize;
      this.maxRecords = maxRecords;
      this.maxRecordsPerBuffer = maxRecordsPerBuffer;
    }

    synchronized long getNumRecordInBuffer() {
      return numRecordInBuffer;
    }

    synchronized long getInSendListBytes() {
      return inSendListBytes;
    }

    synchronized void incrNumRecordInBuffer() {
      numRecordInBuffer++;
    }

    synchronized void setUpForSend(int recordSize, long serializedBytes) {
      numRecordInBuffer -= recordSize;
      inSendListBytes += serializedBytes;
      notifyAll();
    }

    synchronized void descInSendListBytes(long bytes) {
      inSendListBytes -= bytes;
      notifyAll();
    }

    synchronized void updateEstimatedRecordSize(int estimated) {
      estimatedRecordSize = estimatedRecordSize > 0 ? (int) (estimated * 0.6 + estimatedRecordSize * 0.4) : estimated;
      notifyAll();
    }

    synchronized boolean overMemorySize(double ratio) {
      if (estimatedRecordSize > 0) {
        return (numRecordInBuffer * estimatedRecordSize + inSendListBytes) > maxMemSize * ratio;
      } else {
        return numRecordInBuffer > maxRecords * ratio;
      }
    }

    synchronized boolean overMemorySizePerBuffer(int records) {
      if (estimatedRecordSize > 0) {
        return (records * estimatedRecordSize) > maxBufferSize;
      } else {
        return records > maxRecordsPerBuffer;
      }
    }

    synchronized boolean shouldSendBuffer() {
      if (LOG.isDebugEnabled() && overMemorySize(memoryThreshold)) {
        if (inSendListBytes <=  memoryThreshold) {
          LOG.debug("We can send buffer, memoryUsage is " + this);
        } else {
          LOG.debug("We can not send buffer, send buffer limit, memoryUsage is " + this);
        }
      }
      return overMemorySize(memoryThreshold) && inSendListBytes <= maxMemoryForSend;
    }

    synchronized void waitForMemory() throws InterruptedException {
      // The value estimatedRecordSize is not exact value, it's going to change. If the estimatedRecordSize suddenly
      // goes up, numRecordInBuffer * estimatedRecordSize will suddenly be greater than maxMemSize * ratio, then will
      // be stuck forever even though inSendListBytes have been decreased. Here add new condition that
      // inSendListBytes > 0 to avoid stuck.
      while (overMemorySize(1.0) && inSendListBytes > 0) {
        LOG.warn("The memory for records is full, memoryUsage is " + this);
        wait();
      }
    }

    @Override
    public String toString() {
      return "MemoryUsage{" +
          "maxMemSize=" + maxMemSize +
          ", memoryThreshold=" + memoryThreshold +
          ", maxMemoryForSend=" + maxMemoryForSend +
          ", maxBufferSize=" + maxBufferSize +
          ", maxRecords=" + maxRecords +
          ", maxRecordsPerBuffer=" + maxRecordsPerBuffer +
          ", numRecordInBuffer=" + numRecordInBuffer +
          ", inSendListBytes=" + inSendListBytes +
          ", estimatedRecordSize=" + estimatedRecordSize +
          '}';
    }
  }
}
