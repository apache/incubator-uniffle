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

package org.apache.uniffle.storage.handler.impl;

import java.io.File;
import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;

/** A shuffle writer that write data into null device for performance test purpose only. */
public class LocalFileNullDeviceWriter extends LocalFileNioWriter {
  private static final File NULL_DEVICE_FILE = new File("/dev/null");

  @VisibleForTesting
  public LocalFileNullDeviceWriter(File file) throws IOException {
    this(file, 8 * 1024);
  }

  public LocalFileNullDeviceWriter(File file, int bufferSize) throws IOException {
    super(NULL_DEVICE_FILE, bufferSize);
  }
}
