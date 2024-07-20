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

package org.apache.uniffle.server.audit;

import org.slf4j.Logger;

import org.apache.uniffle.common.audit.AuditContext;
import org.apache.uniffle.common.rpc.StatusCode;

/** An audit context for s3 rest service. */
public class ServerAuditContext implements AuditContext {
  private final Logger log;
  private boolean allowed;
  private boolean succeeded;
  private String command;
  private String statusCode;
  private long creationTimeNs;
  private long executionTimeNs;
  private String appId = "N/A";
  private int shuffleId = -1;
  private String args;

  /**
   * Constructor of {@link ServerAuditContext}.
   *
   * @param log the logger to log the audit information
   */
  public ServerAuditContext(Logger log) {
    this.log = log;
    allowed = true;
  }

  /**
   * Sets mCommand field.
   *
   * @param command the command associated with S3 rest service
   * @return this {@link AuditContext} instance
   */
  public ServerAuditContext setCommand(String command) {
    this.command = command;
    return this;
  }

  /**
   * Sets mCreationTimeNs field.
   *
   * @param creationTimeNs the System.nanoTime() when this operation create, it only can be used to
   *     compute operation mExecutionTime
   * @return this {@link AuditContext} instance
   */
  public ServerAuditContext setCreationTimeNs(long creationTimeNs) {
    this.creationTimeNs = creationTimeNs;
    return this;
  }

  /**
   * Sets status code field.
   *
   * @param statusCode the status code
   * @return this {@link AuditContext} instance
   */
  public ServerAuditContext setStatusCode(StatusCode statusCode) {
    this.statusCode = statusCode.name();
    if (statusCode == StatusCode.SUCCESS) {
      setSucceeded(true);
    }
    return this;
  }

  /**
   * Sets status code field.
   *
   * @param statusCode the status code
   * @return this {@link AuditContext} instance
   */
  public ServerAuditContext setStatusCode(String statusCode) {
    this.statusCode = statusCode;
    return this;
  }

  @Override
  public ServerAuditContext setAllowed(boolean allowed) {
    this.allowed = allowed;
    return this;
  }

  @Override
  public ServerAuditContext setSucceeded(boolean succeeded) {
    this.succeeded = succeeded;
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
    return String.format(
        "succeeded=%b\tallowed=%b\tcmd=%s\t"
            + "statusCode=%s\tappId=%s\tshuffleId=%s\texecutionTimeUs=%d\targs{%s}",
        succeeded, allowed, command, statusCode, appId, shuffleId, executionTimeNs / 1000, args);
  }

  public ServerAuditContext setAppId(String appId) {
    this.appId = appId;
    return this;
  }

  public ServerAuditContext setShuffleId(int shuffleId) {
    this.shuffleId = shuffleId;
    return this;
  }

  public ServerAuditContext setArgs(String args) {
    this.args = args;
    return this;
  }
}
