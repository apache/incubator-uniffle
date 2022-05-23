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

package com.tencent.rss.storage;

import com.google.common.collect.Lists;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.storage.handler.impl.HdfsFileReader;
import com.tencent.rss.storage.handler.impl.HdfsShuffleWriteHandler;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HdfsTestBase implements Serializable {

  public static Configuration conf;
  protected static String HDFS_URI;
  protected static FileSystem fs;
  protected static MiniDFSCluster cluster;
  protected static File baseDir;

  @BeforeAll
  public static void setUpHdfs(@TempDir File tempDir) throws IOException {
    conf = new Configuration();
    baseDir = tempDir;
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR,
        baseDir.getAbsolutePath());
    cluster = (new MiniDFSCluster.Builder(conf)).build();
    HDFS_URI = "hdfs://localhost:" + cluster.getNameNodePort() + "/";
    fs = (new Path(HDFS_URI)).getFileSystem(conf);
  }

  @AfterAll
  public static void tearDownHdfs() throws IOException {
    fs.close();
    cluster.shutdown();
  }

  protected void compareBytes(List<byte[]> expected, List<ByteBuffer> actual) {
    assertEquals(expected.size(), actual.size());

    for (int i = 0; i < expected.size(); i++) {
      byte[] expectedI = expected.get(i);
      ByteBuffer bb = actual.get(i);
      for (int j = 0; j < expectedI.length; j++) {
        assertEquals(expectedI[j], bb.get(j));
      }
    }
  }
}
