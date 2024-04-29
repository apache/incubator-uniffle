package org.apache.uniffle.server.merge;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.merger.AbstractSegment;
import org.apache.uniffle.common.merger.Merger;
import org.apache.uniffle.common.merger.Segment;
import org.apache.uniffle.common.records.RecordsReader;
import org.apache.uniffle.common.serializer.PartialInputStream;
import org.apache.uniffle.common.serializer.PartialInputStreamImpl;
import org.apache.uniffle.common.serializer.SerializerUtils;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.storage.handler.api.ShuffleWriteHandler;
import org.apache.uniffle.storage.handler.impl.LocalFileServerReadHandler;
import org.apache.uniffle.storage.handler.impl.LocalFileWriteHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class BlockFlushFileReaderTest {

  private static AtomicInteger ATOMIC_INT = new AtomicInteger(0);

  @ParameterizedTest
  @ValueSource(strings = {
      "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,2",
      "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,4",
      "java.lang.String,java.lang.Integer,2",
      "java.lang.String,java.lang.Integer,8",
      "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer,2",
      "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer,32",
  })
  public void writeTestWithMerge(String classes, @TempDir File tmpDir) throws Exception {
    String[] classArray = classes.split(",");
    Class keyClass = SerializerUtils.getClassByName(classArray[0]);
    Class valueClass = SerializerUtils.getClassByName(classArray[1]);
    Comparator comparator = SerializerUtils.getComparator(keyClass);
    int ringBufferSize = Integer.parseInt(classArray[2]);

    File dataOutput = new File(tmpDir, "dataOutput");
    File dataDir = new File(tmpDir, "data");
    String[] basePaths = new String[] {dataDir.getAbsolutePath()};
    final LocalFileWriteHandler writeHandler1 =
        new LocalFileWriteHandler("appId", 0, 1, 1, basePaths[0], "pre");

    RssBaseConf conf = new RssBaseConf();
    conf.setString("rss.storage.basePath", dataDir.getAbsolutePath());
    final Set<Long> expectedBlockIds = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      writeTestData(generateBlocks(conf, keyClass, valueClass, i, 10, 10090), writeHandler1, expectedBlockIds);
    }

    LocalFileServerReadHandler readHandler =
        new LocalFileServerReadHandler("appId", 0, 1, 1, 10, dataDir.getAbsolutePath());
    String dataFileName = readHandler.getDataFileName();
    String indexFileName = readHandler.getIndexFileName();

    BlockFlushFileReader blockFlushFileReader = new BlockFlushFileReader(dataFileName, indexFileName, ringBufferSize);

    List<AbstractSegment> segments = new ArrayList<>();
    for (Long blockId : expectedBlockIds) {
      PartialInputStream partialInputStream = blockFlushFileReader.registerBlockInputStream(blockId);
      segments.add(new Segment(conf, partialInputStream, blockId, keyClass, valueClass));
    }

    FileOutputStream outputStream = new FileOutputStream(dataOutput);
    Merger.merge(conf, outputStream, segments, keyClass, valueClass, comparator);
    outputStream.close();

    int index = 0;
    RecordsReader reader =
        new RecordsReader(conf, PartialInputStreamImpl.newInputStream(dataOutput, 0, dataOutput.length()),
            keyClass, valueClass);
    while(reader.hasNext()) {
      reader.next();
      assertEquals(SerializerUtils.genData(keyClass, index), reader.getCurrentKey());
      assertEquals(SerializerUtils.genData(valueClass, index), reader.getCurrentValue());
      index++;
    }
    assertEquals(100900, index);
  }

  public static void writeTestData(List<ShufflePartitionedBlock> blocks, ShuffleWriteHandler handler,
                                   Set<Long> expectedBlockIds) throws Exception {
    blocks.forEach(block -> block.getData().retain());
    handler.write(blocks);
    blocks.forEach(block -> expectedBlockIds.add(block.getBlockId()));
    blocks.forEach(block -> block.getData().release());
  }

  public static List<ShufflePartitionedBlock> generateBlocks(RssConf rssConf, Class keyClass, Class valueClass,
                                                             int start, int interval, int length) throws IOException {
    BlockIdLayout layout = BlockIdLayout.DEFAULT;
    List<ShufflePartitionedBlock> blocks = Lists.newArrayList();
    byte[] bytes = SerializerUtils.genSortedRecordBytes(rssConf, keyClass, valueClass, start, interval, length, 1);
    long blockId = layout.getBlockId(ATOMIC_INT.incrementAndGet(), 0, 100);
    blocks.add(new ShufflePartitionedBlock(bytes.length, bytes.length, 0, blockId, 100, bytes));
    return blocks;
  }

  @Test
  public void test2() {
    int size = 1;
    assertEquals(2, Integer.highestOneBit((Math.min(32, Math.max(2, size)) - 1) << 1));
    size = 2;
    assertEquals(2, Integer.highestOneBit((Math.min(32, Math.max(2, size)) - 1) << 1));
    size = 3;
    assertEquals(4, Integer.highestOneBit((Math.min(32, Math.max(2, size)) - 1) << 1));
    size = 4;
    assertEquals(4, Integer.highestOneBit((Math.min(32, Math.max(2, size)) - 1) << 1));
    size = 5;
    assertEquals(8, Integer.highestOneBit((Math.min(32, Math.max(2, size)) - 1) << 1));
    size = 8;
    assertEquals(8, Integer.highestOneBit((Math.min(32, Math.max(2, size)) - 1) << 1));
    size = 9;
    assertEquals(16, Integer.highestOneBit((Math.min(32, Math.max(2, size)) - 1) << 1));
    size = 15;
    assertEquals(16, Integer.highestOneBit((Math.min(32, Math.max(2, size)) - 1) << 1));
    size = 16;
    assertEquals(16, Integer.highestOneBit((Math.min(32, Math.max(2, size)) - 1) << 1));
    size = 20;
    assertEquals(32, Integer.highestOneBit((Math.min(32, Math.max(2, size)) - 1) << 1));
    size = 32;
    assertEquals(32, Integer.highestOneBit((Math.min(32, Math.max(2, size)) - 1) << 1));
    size = 100;
    assertEquals(32, Integer.highestOneBit((Math.min(32, Math.max(2, size)) - 1) << 1));
  }

//  @Test
//  public void test() throws Exception {
//    byte b = -1;
//    int i = b & 0xFF;
//    System.out.println(i);
//    Semaphore semaphore = new Semaphore(0);
//    semaphore.release();
//    System.out.println(semaphore.availablePermits());
//    semaphore.release();
//    System.out.println(semaphore.availablePermits());
//    semaphore.acquire();
//    System.out.println(semaphore.availablePermits());
//    semaphore.acquire();
//    System.out.println(semaphore.availablePermits());
//
//
//
//    ReentrantLock lock = new ReentrantLock(true);
//    lock.lock();
//    System.out.println(lock.isHeldByCurrentThread() + ", hold count " + lock.getHoldCount());
//    lock.lock();
//    System.out.println(lock.isHeldByCurrentThread() + ", hold count " + lock.getHoldCount());
//    System.out.println("lock again!");
//    lock.unlock();
//    System.out.println(lock.isHeldByCurrentThread() + ", hold count " + lock.getHoldCount());
//    lock.unlock();
//    System.out.println(lock.isHeldByCurrentThread() + ", hold count " + lock.getHoldCount());
//    lock.unlock();
//    System.out.println(lock.isHeldByCurrentThread() + ", hold count " + lock.getHoldCount());
//  }
}
