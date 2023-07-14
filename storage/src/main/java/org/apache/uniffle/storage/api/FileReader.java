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

package org.apache.uniffle.storage.api;

import java.nio.ByteBuffer;

public interface FileReader {

  /**
   * This method will return a byte array, will read the length of this file data from offset
   * position.
   *
   * @param offset the file offset which we start to read
   * @param length the data length which we need to read
   * @return file data
   */
  byte[] read(long offset, int length);

  /**
   * This method will return a byte array, will read the data from current position to the end of
   * file
   *
   * @return file data
   */
  byte[] read();

  /**
   * This method will return a direct byte buffer, will read the length of this file data from
   * offset position.
   *
   * @param offset the file offset which we start to read
   * @param length the data length which we need to read
   * @return file data
   */
  ByteBuffer readAsByteBuffer(long offset, int length);

  /**
   * This method will return a direct byte buffer, will read the data from current position to the
   * end of file
   *
   * @return file data
   */
  ByteBuffer readAsByteBuffer();
}
