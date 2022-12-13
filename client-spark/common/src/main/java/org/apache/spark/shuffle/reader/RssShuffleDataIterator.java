/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.reader;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import org.apache.spark.executor.ShuffleReadMetrics;
import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.RssSparkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Product2;
import scala.Tuple2;
import scala.collection.AbstractIterator;
import scala.collection.Iterator;
import scala.runtime.BoxedUnit;

import org.apache.uniffle.client.api.ShuffleReadClient;
import org.apache.uniffle.client.response.CompressedShuffleBlock;
import org.apache.uniffle.client.util.RssClientConfig;
import org.apache.uniffle.common.compression.Codec;
import org.apache.uniffle.common.config.RssConf;

public class RssShuffleDataIterator<K, C> extends AbstractIterator<Product2<K, C>> {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleDataIterator.class);

  private Iterator<Tuple2<Object, Object>> recordsIterator = null;
  private SerializerInstance serializerInstance;
  private ShuffleReadClient shuffleReadClient;
  private ShuffleReadMetrics shuffleReadMetrics;
  private long readTime = 0;
  private long serializeTime = 0;
  private long decompressTime = 0;
  private DeserializationStream deserializationStream = null;
  private ByteBufInputStream byteBufInputStream = null;
  private long compressedBytesLength = 0;
  private long unCompressedBytesLength = 0;
  private ByteBuffer uncompressedData;
  private Codec codec;

  public RssShuffleDataIterator(
      Serializer serializer,
      ShuffleReadClient shuffleReadClient,
      ShuffleReadMetrics shuffleReadMetrics,
      RssConf rssConf) {
    this.serializerInstance = serializer.newInstance();
    this.shuffleReadClient = shuffleReadClient;
    this.shuffleReadMetrics = shuffleReadMetrics;
    this.codec = Codec.newInstance(rssConf);
    // todo: support off-heap bytebuffer
    this.uncompressedData = ByteBuffer.allocate(
        (int) rssConf.getSizeAsBytes(
            RssClientConfig.RSS_WRITER_BUFFER_SIZE,
            RssSparkConfig.RSS_WRITER_BUFFER_SIZE.defaultValueString()
        )
    );
  }

  public Iterator<Tuple2<Object, Object>> createKVIterator(ByteBuffer data, int size) {
    clearDeserializationStream();
    byteBufInputStream = new ByteBufInputStream(Unpooled.wrappedBuffer(data.array(), 0, size), true);
    deserializationStream = serializerInstance.deserializeStream(byteBufInputStream);
    return deserializationStream.asKeyValueIterator();
  }

  private void clearDeserializationStream() {
    if (byteBufInputStream != null) {
      try {
        byteBufInputStream.close();
      } catch (IOException e) {
        LOG.warn("Can't close ByteBufInputStream, memory may be leaked.");
      }
    }
    if (deserializationStream != null) {
      deserializationStream.close();
    }
    deserializationStream = null;
    byteBufInputStream = null;
  }

  @Override
  public boolean hasNext() {
    if (recordsIterator == null || !recordsIterator.hasNext()) {
      // read next segment
      long startFetch = System.currentTimeMillis();
      CompressedShuffleBlock compressedBlock = shuffleReadClient.readShuffleBlockData();
      // If ShuffleServer delete

      ByteBuffer compressedData = null;
      if (compressedBlock != null) {
        compressedData = compressedBlock.getByteBuffer();
      }
      long fetchDuration = System.currentTimeMillis() - startFetch;
      shuffleReadMetrics.incFetchWaitTime(fetchDuration);
      if (compressedData != null) {
        long compressedDataLength = compressedData.limit() - compressedData.position();
        compressedBytesLength += compressedDataLength;
        shuffleReadMetrics.incRemoteBytesRead(compressedDataLength);

        int uncompressedLen = compressedBlock.getUncompressLength();
        if (uncompressedData == null || uncompressedData.capacity() < uncompressedLen) {
          uncompressedData = ByteBuffer.allocate(uncompressedLen);
        }
        uncompressedData.clear();
        long startDecompress = System.currentTimeMillis();
        codec.decompress(compressedData, uncompressedLen, uncompressedData, 0);
        unCompressedBytesLength += compressedBlock.getUncompressLength();
        long decompressDuration = System.currentTimeMillis() - startDecompress;
        decompressTime += decompressDuration;
        // create new iterator for shuffle data
        long startSerialization = System.currentTimeMillis();
        recordsIterator = createKVIterator(uncompressedData, uncompressedLen);
        long serializationDuration = System.currentTimeMillis() - startSerialization;
        readTime += fetchDuration;
        serializeTime += serializationDuration;
      } else {
        // finish reading records, check data consistent
        shuffleReadClient.checkProcessedBlockIds();
        shuffleReadClient.logStatics();
        LOG.info("Fetch " + compressedBytesLength + " bytes cost " + readTime + " ms and "
            + serializeTime + " ms to serialize, " + decompressTime + " ms to decompress with unCompressionLength["
            + unCompressedBytesLength + "]");
        return false;
      }
    }
    return recordsIterator.hasNext();
  }

  @Override
  public Product2<K, C> next() {
    shuffleReadMetrics.incRecordsRead(1L);
    return (Product2<K, C>) recordsIterator.next();
  }

  public BoxedUnit cleanup() {
    clearDeserializationStream();
    if (shuffleReadClient != null) {
      shuffleReadClient.close();
    }
    shuffleReadClient = null;
    uncompressedData = null;
    return BoxedUnit.UNIT;
  }

  @VisibleForTesting
  protected ShuffleReadMetrics getShuffleReadMetrics() {
    return shuffleReadMetrics;
  }
}

