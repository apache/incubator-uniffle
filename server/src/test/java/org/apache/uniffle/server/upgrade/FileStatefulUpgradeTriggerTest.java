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
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class FileStatefulUpgradeTriggerTest {

  @Test
  public void test(@TempDir File dir) throws IOException {
    String path = dir.getAbsolutePath() + "/upgrade.status.file";
    FileStatefulUpgradeTrigger trigger = new FileStatefulUpgradeTrigger(path);
    AtomicReference<Boolean> finished = new AtomicReference<>(false);
    trigger.register(() -> finished.set(true));
    Awaitility.await().timeout(Durations.TWO_SECONDS).until(() -> !finished.get());
    File file = new File(path);
    file.createNewFile();
    Awaitility.await().timeout(Durations.TWO_SECONDS).until(() -> finished.get());
  }
}
