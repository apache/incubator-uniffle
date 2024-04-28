package org.apache.uniffle.server.merge;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.netty.buffer.FileSegmentManagedBuffer;
import org.apache.uniffle.common.serializer.PartialInputStream;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.storage.common.FileBasedShuffleSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.FileChannelImpl;

public class BlockFlushFileReader {

  private static final Logger LOG = LoggerFactory.getLogger(BlockFlushFileReader.class);
  private static final int BUFFER_SIZE = 4096;

  private final String dataFile;
  private final String indexFile;
  private FileInputStream dataInput;
  private FileChannelImpl dataFileChannel;
  boolean stop = false;

  // blockid -> BlockInputStream
  private final Map<Long, BlockInputStream> inputStreamMap = JavaUtils.newConcurrentMap();
  private LinkedHashMap<Long, FileBasedShuffleSegment> indexSegments = new LinkedHashMap<>();

  private FlushFileReader flushFileReader;
  private volatile Throwable readThrowable = null;
  // Even though there are many BlockInputStream, these BlockInputStream must
  // be executed in the same thread. When BlockInputStream have been read out,
  // we can notify flushFileReader by lock. Then flushFileReader load the buffer,
  // and block BlockInputStream by lock until flushFileReader load done.
  private final ReentrantLock lock = new ReentrantLock(true);

  public BlockFlushFileReader(String dataFile, String indexFile) throws IOException {
    // Make sure flush file will not be updated
    this.dataFile = dataFile;
    this.indexFile = indexFile;
    loadShuffleIndex();
    this.dataInput = new FileInputStream(dataFile);
    this.dataFileChannel = (FileChannelImpl) dataInput.getChannel();
    // Avoid flushFileReader noop loop
    this.lock.lock();
    this.flushFileReader = new FlushFileReader();
    this.flushFileReader.start();
  }

  public void loadShuffleIndex() {
    File indexFile = new File(this.indexFile);
    long indexFileSize = indexFile.length();
    int indexNum = (int) (indexFileSize / FileBasedShuffleSegment.SEGMENT_SIZE);
    int len = indexNum * FileBasedShuffleSegment.SEGMENT_SIZE;
    ByteBuffer indexData = new FileSegmentManagedBuffer(indexFile, 0, len).nioByteBuffer();
    while (indexData.hasRemaining()) {
      long offset = indexData.getLong();
      int length = indexData.getInt();
      int uncompressLength = indexData.getInt();
      long crc = indexData.getLong();
      long blockId = indexData.getLong();
      long taskAttemptId = indexData.getLong();
      FileBasedShuffleSegment fileBasedShuffleSegment =
          new FileBasedShuffleSegment(blockId, offset, length, uncompressLength, crc, taskAttemptId);
      indexSegments.put(fileBasedShuffleSegment.getBlockId(), fileBasedShuffleSegment);
    }
  }

  public void close() throws IOException, InterruptedException {
    if (!this.stop) {
      stop = true;
      flushFileReader.interrupt();
      flushFileReader = null;
    }
    if (dataInput != null) {
      this.dataInput.close();
      this.dataInput = null;
    }
  }

  public BlockInputStream registerBlockInputStream(long blockId) {
    if (!indexSegments.containsKey(blockId)) {
      return null;
    }
    if (!inputStreamMap.containsKey(blockId)) {
      inputStreamMap.put(blockId, new BlockInputStream(blockId, this.indexSegments.get(blockId).getLength()));
    }
    return inputStreamMap.get(blockId);
  }

  class FlushFileReader extends Thread {
    @Override
    public void run() {
      while (!stop) {
        int available = 0;
        int process = 0;
        lock.lock();
        try {
          Iterator<Map.Entry<Long, FileBasedShuffleSegment>> iterator = indexSegments.entrySet().iterator();
          while (iterator.hasNext()) {
            FileBasedShuffleSegment segment = iterator.next().getValue();
            long blockId = segment.getBlockId();
            BlockInputStream inputStream = inputStreamMap.get(blockId);
            if (inputStream == null || inputStream.eof) {
              continue;
            }
            available ++;
            if (inputStream.getCacheBuffer().readable()) {
              continue;
            }
            process ++;
            long off = segment.getOffset() + inputStream.getLoadPos();
            if (dataFileChannel.position() != off) {
              dataFileChannel.position(off);
            }
            inputStream.loadCacheBuffer();
          }
        } catch (Throwable throwable) {
          readThrowable = throwable;
          LOG.info("FlushFileReader read failed, caused by ", throwable);
          stop = true;
        } finally {
          lock.unlock();
          LOG.info("statistics: load buffer available is {}, process is {}", available, process);
        }
      }
    }
  }

  class Buffer {

    private byte[] bytes = new byte[BUFFER_SIZE];
    private int cap = BUFFER_SIZE;
    private int pos = cap;


    public int get() {
      return this.bytes[pos++] & 0xFF;
    }

    public boolean readable() {
      return pos < cap;
    }

    public void writeBuffer(FileChannelImpl fc, int length) throws IOException {
      fc.read(ByteBuffer.wrap(this.bytes, 0, length));
      this.pos = 0;
      this.cap = length;
    }
  }

  public class BlockInputStream extends PartialInputStream {

    private long blockId;
    private volatile Buffer readBuffer;
    private volatile Buffer cacheBuffer;
    private boolean eof = false;
    private final int length;
    // pos is read position
    private int pos = 0;
    // loadPos is the position which have been loaded.
    private int loadPos = 0;

    public BlockInputStream(long blockId, int length) {
      this.blockId = blockId;
      this.length = length;
      this.readBuffer = new Buffer();
      this.cacheBuffer = new Buffer();
    }

    @Override
    public int available() throws IOException {
      return length - pos;
    }

    public long getStart() {
      return 0;
    }

    public long getEnd() {
      return length;
    }

    public long getLoadPos() {
      return this.loadPos;
    }

    @Override
    public void close() throws IOException {
      try {
        inputStreamMap.remove(blockId);
        if (inputStreamMap.size() == 0) {
          BlockFlushFileReader.this.close();
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    public void loadCacheBuffer() throws IOException {
      int size = Math.min(available(), BUFFER_SIZE);
      this.loadPos += size;
      cacheBuffer.writeBuffer(dataFileChannel, size);
    }

    public void flipBuffer() {
      Buffer tmp = this.readBuffer;
      this.readBuffer = this.cacheBuffer;
      this.cacheBuffer = tmp;
    }

    public Buffer getCacheBuffer() {
      return cacheBuffer;
    }

    @Override
    public int read() throws IOException {
      try {
        if (readThrowable != null) {
          throw new IOException("Read flush file failed!", readThrowable);
        }
        if (eof) {
          return -1;
        }
        while (!this.readBuffer.readable() && !this.cacheBuffer.readable()) {
          if (lock.isHeldByCurrentThread()) {
            lock.unlock();
          }
          lock.lock();
        }
        if (!this.readBuffer.readable()) {
          flipBuffer();
        }
        int c = this.readBuffer.get();
        pos++;
        if (pos >= length) {
          eof = true;
        }
        return c;
      } catch (Throwable e) {
        throw new IOException(e);
      }
    }
  }
}
