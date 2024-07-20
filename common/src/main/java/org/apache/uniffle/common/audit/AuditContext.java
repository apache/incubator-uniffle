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

/** Context for audit logging. */
public interface AuditContext extends Closeable {

  /**
   * Set to true if the operation associated with this {@link AuditContext} is allowed, false
   * otherwise.
   *
   * @param allowed true if operation is allowed, false otherwise
   * @return {@link AuditContext} instance itself
   */
  AuditContext setAllowed(boolean allowed);

  /**
   * Set to true if the operration associated with this {@link AuditContext} is allowed and
   * succeeds.
   *
   * @param succeeded true if the operation has succeeded, false otherwise
   * @return {@link AuditContext} instance itself
   */
  AuditContext setSucceeded(boolean succeeded);

  @Override
  void close();
}
