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

package org.apache.uniffle.common.serializer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/*
 * PartialInputStream is a configurable partial input stream, which
 * only allows reading from start to end of the source input stream.
 * */
public abstract class PartialInputStream extends InputStream {

  @Override
  public abstract int available() throws IOException;

  public abstract long getStart();

  public abstract long getEnd();

  public static PartialInputStream newInputStream(File file, long start, long end)
      throws IOException {
    FileInputStream input = new FileInputStream(file);
    FileChannel fc = input.getChannel();
    if (fc == null) {
      throw new NullPointerException("channel is null!");
    }
    long size = fc.size();
    return new PartialInputStreamImpl(
        fc,
        start,
        Math.min(end, size),
        () -> {
          input.close();
        });
  }

  public static PartialInputStream newInputStream(File file) throws IOException {
    return PartialInputStream.newInputStream(file, 0, file.length());
  }

  public static PartialInputStream newInputStream(ByteBuffer byteBuffer, long start, long end)
      throws IOException {
    return new BufferPartialInputStreamImpl(byteBuffer, start, Math.min(byteBuffer.limit(), end));
  }

  public static PartialInputStream newInputStream(ByteBuffer byteBuffer) throws IOException {
    return new BufferPartialInputStreamImpl(byteBuffer, 0, byteBuffer.limit());
  }
}
