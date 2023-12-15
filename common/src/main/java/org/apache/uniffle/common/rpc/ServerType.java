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

package org.apache.uniffle.common.rpc;

/** This should sync/match with how ClientType changes */
public enum ServerType {
  GRPC(1),
  NETTY(2),
  GRPC_NETTY(3);

  private int val;

  ServerType(int val) {
    this.val = val;
  }

  private int getVal() {
    return val;
  }

  public boolean withGrpc() {
    return (this.getVal() & GRPC.getVal()) != 0;
  }

  public boolean withNetty() {
    return (this.getVal() & NETTY.getVal()) != 0;
  }
}
