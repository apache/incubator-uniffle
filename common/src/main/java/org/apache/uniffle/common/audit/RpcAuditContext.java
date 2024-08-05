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

package org.apache.uniffle.common.audit;

import java.io.Closeable;

import org.slf4j.Logger;

import org.apache.uniffle.common.rpc.StatusCode;

/** Context for rpc audit logging. */
public abstract class RpcAuditContext implements Closeable {
  private final Logger log;
  private String command;
  private String statusCode;
  private String args;
  private String from;
  private long creationTimeNs;
  protected long executionTimeNs;

  public RpcAuditContext(Logger log) {
    this.log = log;
  }

  protected abstract String content();

  /**
   * Sets mCommand field.
   *
   * @param command the command associated with shuffle server rpc
   * @return this {@link RpcAuditContext} instance
   */
  public RpcAuditContext setCommand(String command) {
    this.command = command;
    return this;
  }

  /**
   * Sets creationTimeNs field.
   *
   * @param creationTimeNs the System.nanoTime() when this operation create, it only can be used to
   *     compute operation mExecutionTime
   * @return this {@link RpcAuditContext} instance
   */
  public RpcAuditContext setCreationTimeNs(long creationTimeNs) {
    this.creationTimeNs = creationTimeNs;
    return this;
  }

  /**
   * Sets status code field.
   *
   * @param statusCode the status code
   * @return this {@link RpcAuditContext} instance
   */
  public RpcAuditContext setStatusCode(StatusCode statusCode) {
    if (statusCode == null) {
      this.statusCode = "UNKNOWN";
    } else {
      this.statusCode = statusCode.name();
    }
    return this;
  }

  /**
   * Sets status code field.
   *
   * @param statusCode the status code
   * @return this {@link RpcAuditContext} instance
   */
  public RpcAuditContext setStatusCode(org.apache.uniffle.proto.RssProtos.StatusCode statusCode) {
    if (statusCode == null) {
      this.statusCode = "UNKNOWN";
    } else {
      this.statusCode = statusCode.name();
    }
    return this;
  }

  /**
   * Sets status code field.
   *
   * @param statusCode the status code
   * @return this {@link RpcAuditContext} instance
   */
  public RpcAuditContext setStatusCode(String statusCode) {
    this.statusCode = statusCode;
    return this;
  }

  public RpcAuditContext setArgs(String args) {
    this.args = args;
    return this;
  }

  public RpcAuditContext setFrom(String from) {
    this.from = from;
    return this;
  }

  @Override
  public void close() {
    if (log == null) {
      return;
    }
    executionTimeNs = System.nanoTime() - creationTimeNs;
    log.info(toString());
  }

  @Override
  public String toString() {
    String line =
        String.format(
            "cmd=%s\tstatusCode=%s\tfrom=%s\texecutionTimeUs=%d\t%s",
            command, statusCode, from, executionTimeNs / 1000, content());
    if (args != null) {
      line += String.format("\targs{%s}", args);
    }
    return line;
  }
}
