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

package org.apache.uniffle.flink.reader.virtual;

import java.io.IOException;

import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.CloseableIterator;

public class FakedRssChannelStateWriter implements ChannelStateWriter {
  @Override
  public void start(long checkpointId, CheckpointOptions checkpointOptions) {}

  @Override
  public void addInputData(
      long checkpointId, InputChannelInfo info, int startSeqNum, CloseableIterator<Buffer> data) {}

  @Override
  public void addOutputData(
      long checkpointId, ResultSubpartitionInfo info, int startSeqNum, Buffer... data)
      throws IllegalArgumentException {}

  @Override
  public void finishInput(long checkpointId) {}

  @Override
  public void finishOutput(long checkpointId) {}

  @Override
  public void abort(long checkpointId, Throwable cause, boolean cleanup) {}

  @Override
  public ChannelStateWriteResult getAndRemoveWriteResult(long checkpointId)
      throws IllegalArgumentException {
    return null;
  }

  @Override
  public void close() throws IOException {}
}
