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

package org.apache.tez.common;

import org.apache.tez.dag.records.TezTaskAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdUtils {
  private static final Logger LOG = LoggerFactory.getLogger(IdUtils.class);

  private IdUtils() {}

  /**
   * @param pathComponent, like: attempt_1681717153064_2768836_2_00_000000_0_10006
   * @return remove last 6 char, return TezTaskAttemptID
   */
  public static TezTaskAttemptID convertTezTaskAttemptID(String pathComponent) {
    LOG.info("convertTezTaskAttemptID, pathComponent:{}", pathComponent);
    return TezTaskAttemptID.fromString(pathComponent.substring(0, pathComponent.length() - 6));
  }
}
