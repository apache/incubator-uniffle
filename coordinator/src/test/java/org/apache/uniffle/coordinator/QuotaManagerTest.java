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

package org.apache.uniffle.coordinator;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * QuotaManager is a manager for resource restriction.
 */
public class QuotaManagerTest {
  public QuotaManager quotaManager;
  private static final Configuration hdfsConf = new Configuration();
  private static MiniDFSCluster cluster;
  @TempDir
  private static File remotePath = new File("hdfs://rss");

  @BeforeEach
  public void setUp() throws IOException {
    hdfsConf.set("fs.defaultFS", remotePath.getAbsolutePath());
    hdfsConf.set("dfs.nameservices", "rss");
    hdfsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, remotePath.getAbsolutePath());
    cluster = (new MiniDFSCluster.Builder(hdfsConf)).build();
    CoordinatorConf conf = new CoordinatorConf();
    quotaManager = new QuotaManager(conf);
  }

  @AfterAll
  public static void clear() {
    cluster.close();
  }

  @Test
  public void testDetectUserResource() throws Exception {
    final String quotaFile =
        new Path(remotePath.getAbsolutePath()).getFileSystem(hdfsConf).getName() + "/quotaFile.properties";
    final FSDataOutputStream fsDataOutputStream =
        new Path(remotePath.toString()).getFileSystem(hdfsConf).create(new Path(quotaFile));
    String quota1 = "user1 =10";
    String quota2 = "user2= 20";
    String quota3 = "user3 = 30";
    fsDataOutputStream.write(quota1.getBytes(StandardCharsets.UTF_8));
    fsDataOutputStream.write("\n".getBytes(StandardCharsets.UTF_8));
    fsDataOutputStream.write(quota2.getBytes(StandardCharsets.UTF_8));
    fsDataOutputStream.write("\n".getBytes(StandardCharsets.UTF_8));
    fsDataOutputStream.write(quota3.getBytes(StandardCharsets.UTF_8));
    fsDataOutputStream.flush();
    fsDataOutputStream.close();
    CoordinatorConf conf = new CoordinatorConf();
    conf.set(CoordinatorConf.COORDINATOR_QUOTA_DEFAULT_PATH,
        quotaFile);
    quotaManager = new QuotaManager(conf);
    quotaManager.detectUserResource();

    Integer user1 = quotaManager.getDefaultUserApps().get("user1");
    Integer user2 = quotaManager.getDefaultUserApps().get("user2");
    Integer user3 = quotaManager.getDefaultUserApps().get("user3");
    assertEquals(user1, 10);
    assertEquals(user2, 20);
    assertEquals(user3, 30);
  }
}
