/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available. 
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.spark.shuffle.reader;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.esotericsoftware.kryo.io.Input;
import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import org.apache.spark.executor.ShuffleReadMetrics;
import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.RssShuffleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Product2;
import scala.Tuple2;
import scala.collection.AbstractIterator;
import scala.collection.Iterator;

import com.tencent.rss.client.api.ShuffleReadClient;
import com.tencent.rss.client.response.CompressedShuffleBlock;

public class RssShuffleDataIterator<K, C> extends AbstractIterator<Product2<K, C>> {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleDataIterator.class);

  private Iterator<Tuple2<Object, Object>> recordsIterator = null;
  private SerializerInstance serializerInstance;
  private ShuffleReadClient shuffleReadClient;
  private ShuffleReadMetrics shuffleReadMetrics;
  private long readTime = 0;
  private long serializeTime = 0;
  private long decompressTime = 0;
  private Input deserializationInput = null;
  private DeserializationStream deserializationStream = null;
  private ByteBufInputStream byteBufInputStream = null;
  private long unCompressionLength = 0;

  public RssShuffleDataIterator(
      Serializer serializer,
      ShuffleReadClient shuffleReadClient,
      ShuffleReadMetrics shuffleReadMetrics) {
    this.serializerInstance = serializer.newInstance();
    this.shuffleReadClient = shuffleReadClient;
    this.shuffleReadMetrics = shuffleReadMetrics;
  }

  public Iterator<Tuple2<Object, Object>> createKVIterator(ByteBuffer data) {
    clearDeserializationStream();
    byteBufInputStream = new ByteBufInputStream(Unpooled.wrappedBuffer(data), true);
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
    if (deserializationInput != null) {
      deserializationInput.close();
    }
    if (deserializationStream != null) {
      deserializationStream.close();
    }
    deserializationInput = null;
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
        shuffleReadMetrics.incRemoteBytesRead(compressedData.limit() - compressedData.position());
        long startDecompress = System.currentTimeMillis();
        ByteBuffer uncompressedData = RssShuffleUtils.decompressData(
            compressedData, compressedBlock.getUncompressLength());
        unCompressionLength += compressedBlock.getUncompressLength();
        long decompressDuration = System.currentTimeMillis() - startDecompress;
        decompressTime += decompressDuration;
        // create new iterator for shuffle data
        long startSerialization = System.currentTimeMillis();
        recordsIterator = createKVIterator(uncompressedData);
        long serializationDuration = System.currentTimeMillis() - startSerialization;
        readTime += fetchDuration;
        serializeTime += serializationDuration;
      } else {
        // finish reading records, close related reader and check data consistent
        clearDeserializationStream();
        shuffleReadClient.close();
        shuffleReadClient.checkProcessedBlockIds();
        shuffleReadClient.logStatics();
        LOG.info("Fetch " + shuffleReadMetrics.remoteBytesRead() + " bytes cost " + readTime + " ms and "
            + serializeTime + " ms to serialize, " + decompressTime + " ms to decompress with unCompressionLength["
            + unCompressionLength + "]");
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

  @VisibleForTesting
  protected ShuffleReadMetrics getShuffleReadMetrics() {
    return shuffleReadMetrics;
  }
}

