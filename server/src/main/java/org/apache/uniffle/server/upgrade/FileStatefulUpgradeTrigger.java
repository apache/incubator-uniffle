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

import java.io.File;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.uniffle.server.ShuffleServerConf.STATEFUL_UPGRADE_TRIGGER_STATUS_FILE_PATH;

public class FileStatefulUpgradeTrigger implements StatefulUpgradeTrigger {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileStatefulUpgradeTrigger.class);

  private String statusFilePath;

  public FileStatefulUpgradeTrigger(String statusFilePath) {
    if (StringUtils.isEmpty(statusFilePath)) {
      throw new RuntimeException(
          "The conf of " + STATEFUL_UPGRADE_TRIGGER_STATUS_FILE_PATH.key()
              + " must be specified when file-existence based trigger is enabled.");
    }
    this.statusFilePath = statusFilePath;
    LOGGER.info("The {} is enabled", this.getClass().getSimpleName());
  }

  @Override
  public void register(Runnable handler) {
    new Thread(() -> {
      while (true) {
        File file = new File(statusFilePath);
        if (file.exists()) {
          LOGGER.info("Capture the file existence, this will trigger stateful upgrade, path: {}", statusFilePath);
          file.delete();
          handler.run();
          break;
        }

        try {
          Thread.sleep(1000);
        } catch (InterruptedException interruptedException) {
          // ignore
        }
      }
    }).start();
  }
}
