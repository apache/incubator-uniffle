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

package org.apache.uniffle.server.upgrade;

import org.apache.uniffle.server.ShuffleServerConf;

import static org.apache.uniffle.server.ShuffleServerConf.STATEFUL_UPGRADE_TRIGGER_STATUS_FILE_PATH;
import static org.apache.uniffle.server.ShuffleServerConf.STATEFUL_UPGRADE_TRIGGER_TYPE;

public class StatefulUpgradeTriggerFactory {

  public enum Type {
    FILE
  }

  private StatefulUpgradeTriggerFactory() {
    // ignore
  }

  private static class LazyHolder {
    static final StatefulUpgradeTriggerFactory INSTANCE = new StatefulUpgradeTriggerFactory();
  }

  public static StatefulUpgradeTriggerFactory getInstance() {
    return StatefulUpgradeTriggerFactory.LazyHolder.INSTANCE;
  }

  public StatefulUpgradeTrigger get(ShuffleServerConf shuffleServerConf) {
    Type type = shuffleServerConf.get(STATEFUL_UPGRADE_TRIGGER_TYPE);
    switch (type) {
      case FILE:
      default:
        return new FileStatefulUpgradeTrigger(shuffleServerConf.get(STATEFUL_UPGRADE_TRIGGER_STATUS_FILE_PATH));
    }
  }
}
