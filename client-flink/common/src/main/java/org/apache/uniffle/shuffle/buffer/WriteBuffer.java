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

package org.apache.uniffle.shuffle.buffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;

/**
 * Data of different channels can be appended to a {@link WriteBuffer} and after the {@link
 * WriteBuffer} is full or finished, the appended data can be copied from it in channel index order.
 *
 * <p>The lifecycle of a {@link WriteBuffer} can be: new, write, [read, reset, write], finish, read,
 * release. There can be multiple [read, reset, write] operations before finish.
 */
public interface WriteBuffer {

  /**
   * Appends data of the specified channel to this {@link WriteBuffer} and returns true if this
   * {@link WriteBuffer} is full.
   */
  boolean append(ByteBuffer source, int targetChannel, Buffer.DataType dataType) throws IOException;

  /**
   * Copies data in this {@link WriteBuffer} to the target {@link MemorySegment} in channel index
   * order and returns {@link BufferWithChannel} which contains the copied data and the
   * corresponding channel index.
   */
  BufferWithChannel getNextBuffer(@Nullable MemorySegment transitBuffer);

  /** Returns the total number of records written to this {@link WriteBuffer}. */
  long numTotalRecords();

  /** Returns the total number of bytes written to this {@link WriteBuffer}. */
  long numTotalBytes();

  /** Finishes this {@link WriteBuffer} which means no record can be appended any more. */
  void finish();

  /** Whether this {@link WriteBuffer} is finished or not. */
  boolean isFinished();

  /** Releases this {@link WriteBuffer} which releases all resources. */
  void release();

  /** Whether this {@link WriteBuffer} is released or not. */
  boolean isReleased();

  /** Resets this {@link WriteBuffer} to be reused for data appending. */
  void reset();

  /** Returns true if there is still data can be consumed in this {@link WriteBuffer}. */
  boolean hasRemaining();
}
