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

package org.apache.uniffle.common.netty.protocol;

import org.apache.uniffle.common.netty.buffer.ManagedBuffer;

public abstract class RequestMessage extends Message {
  private final long requestId;
  public static final int REQUEST_ID_ENCODE_LENGTH = Long.BYTES;

  public RequestMessage(long requestId) {
    this(requestId, null);
  }

  public RequestMessage(long requestId, ManagedBuffer managedBuffer) {
    super(managedBuffer);
    this.requestId = requestId;
  }

  public long getRequestId() {
    return requestId;
  }

  public abstract String getOperationType();
}
