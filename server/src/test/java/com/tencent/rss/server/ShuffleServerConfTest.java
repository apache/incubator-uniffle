/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available. 
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.rss.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.rules.ExpectedException;

public class ShuffleServerConfTest {

  private static final String confFile = ClassLoader.getSystemResource("confTest.conf").getFile();
  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void defaultConfTest() {
    ShuffleServerConf shuffleServerConf = new ShuffleServerConf();
    assertFalse(shuffleServerConf.loadConfFromFile(null));
    assertEquals("GRPC", shuffleServerConf.getString(ShuffleServerConf.RPC_SERVER_TYPE));
    assertEquals(256, shuffleServerConf.getInteger(ShuffleServerConf.JETTY_CORE_POOL_SIZE));
    assertFalse(shuffleServerConf.getBoolean(ShuffleServerConf.USE_MULTI_STORAGE));
    assertFalse(shuffleServerConf.getBoolean(ShuffleServerConf.UPLOADER_ENABLE));
  }

  @Test
  public void envConfTest() {
    environmentVariables.set("RSS_HOME", (new File(confFile)).getParent());
    ShuffleServerConf shuffleServerConf = new ShuffleServerConf(null);
    assertEquals(1234, shuffleServerConf.getInteger(ShuffleServerConf.RPC_SERVER_PORT));
    assertEquals("HDFS", shuffleServerConf.getString(ShuffleServerConf.RSS_STORAGE_TYPE));
    assertEquals("/var/tmp/test", shuffleServerConf.getString(ShuffleServerConf.RSS_STORAGE_BASE_PATH));
    environmentVariables.set("RSS_HOME", (new File(confFile)).getParent() + "/wrong_dir/");
    assertFalse(shuffleServerConf.loadConfFromFile(null));
  }

  @Test
  public void confTest() {
    ShuffleServerConf shuffleServerConf = new ShuffleServerConf(confFile);
    assertEquals(1234, shuffleServerConf.getInteger(ShuffleServerConf.RPC_SERVER_PORT));
    assertEquals("FILE", shuffleServerConf.getString(ShuffleServerConf.RSS_STORAGE_TYPE));
    assertEquals("/var/tmp/test", shuffleServerConf.getString(ShuffleServerConf.RSS_STORAGE_BASE_PATH));
    assertFalse(shuffleServerConf.loadConfFromFile("/var/tmp/null"));
    assertEquals(2, shuffleServerConf.getLong(ShuffleServerConf.SERVER_BUFFER_CAPACITY));
    assertEquals("value1", shuffleServerConf.getString("rss.server.hadoop.a.b", ""));
    assertEquals("", shuffleServerConf.getString("rss.server.had.a.b", ""));
    assertEquals("COS", shuffleServerConf.getString(ShuffleServerConf.UPLOAD_STORAGE_TYPE));
    assertEquals("GRPC", shuffleServerConf.getString(ShuffleServerConf.RPC_SERVER_TYPE));
    assertTrue(shuffleServerConf.getBoolean(ShuffleServerConf.USE_MULTI_STORAGE));
    assertTrue(shuffleServerConf.getBoolean(ShuffleServerConf.UPLOADER_ENABLE));
    assertEquals(8L, shuffleServerConf.getLong(ShuffleServerConf.REFERENCE_UPLOAD_SPEED_MBS));
    assertEquals(
        1024L * 1024L * 1024L, shuffleServerConf.getLong(ShuffleServerConf.SHUFFLE_MAX_UPLOAD_SIZE));
    assertEquals(13, shuffleServerConf.getInteger(ShuffleServerConf.UPLOADER_THREAD_NUM));

    thrown.expect(NullPointerException.class);
    shuffleServerConf.getInteger(ShuffleServerConf.SERVER_PARTITION_BUFFER_SIZE);

  }
}
