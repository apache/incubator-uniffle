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

package org.apache.uniffle.storage.common;

import java.io.File;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShuffleFileInfoTest {

  @Test
  public void test() throws Exception {
    ShuffleFileInfo shuffleFileInfo = new ShuffleFileInfo();
    shuffleFileInfo.getDataFiles().add(File.createTempFile("dummy-data-file", ".data"));
    shuffleFileInfo.setKey("key");
    assertFalse(shuffleFileInfo.isValid());

    shuffleFileInfo.getIndexFiles().add(File.createTempFile("dummy-data-file", ".index"));
    shuffleFileInfo.getPartitions().add(12);
    shuffleFileInfo.setSize(1024 * 1024 * 32);
    assertTrue(shuffleFileInfo.isValid());
    assertFalse(shuffleFileInfo.shouldCombine(32));
  }
}
