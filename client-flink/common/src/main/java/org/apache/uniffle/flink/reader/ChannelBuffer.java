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

package org.apache.uniffle.flink.reader;

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.buffer.Buffer;

public class ChannelBuffer {

  private Buffer buffer;
  private InputChannelInfo inputChannelInfo;

  public ChannelBuffer() {}

  public ChannelBuffer(Buffer buffer, InputChannelInfo inputChannelInfo) {
    this.buffer = buffer;
    this.inputChannelInfo = inputChannelInfo;
  }

  public Buffer getBuffer() {
    return buffer;
  }

  public void setBuffer(Buffer buffer) {
    this.buffer = buffer;
  }

  public InputChannelInfo getInputChannelInfo() {
    return inputChannelInfo;
  }

  public void setInputChannelInfo(InputChannelInfo inputChannelInfo) {
    this.inputChannelInfo = inputChannelInfo;
  }
}
