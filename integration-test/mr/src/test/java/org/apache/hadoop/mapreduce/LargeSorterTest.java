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

package org.apache.hadoop.mapreduce;

import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServerConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.util.ToolRunner;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class LargeSorterTest extends MRIntegrationTestBase {

  @BeforeClass
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @Test
  public void testAppMasterStart() throws Exception {
    URL url = LargeSorterTest.class.getResource("/");
    String parentPath = new Path(url.getPath()).getParent()
        .getParent().getParent().getParent().toString();
    Configuration conf = new Configuration(mrYarnCluster.getConfig());
    if (System.getenv("JAVA_HOME") == null) {
      throw new RuntimeException("We must set JAVA_HOME");
    }
    conf.set(MRJobConfig.MR_AM_COMMAND_OPTS, "-XX:+TraceClassLoading org.apache.hadoop.mapreduce.v2.app.RssMRAppMaster");
    conf.setInt(MRJobConfig.MAP_MEMORY_MB, 2048);
    conf.setInt(MRJobConfig.IO_SORT_MB, 128);
    conf.setInt(LargeSorter.NUM_MAP_TASKS, 1);
    conf.setInt(LargeSorter.MBS_PER_MAP, 128);
    File file = new File(parentPath, "client-mr/target/shaded");
    File[] jars = file.listFiles();
    File localFile = null;
    for (File jar : jars) {
      if (jar.getName().startsWith("rss-client-mr")) {
        localFile = jar;
        break;
      }
    }
    assertNotNull(localFile);
    String props = System.getProperty("java.class.path");
    String newProps = "";
    String[] splittedProps = props.split(":");
    for (String prop : splittedProps)  {
      if (!prop.contains("classes") && !prop.contains("grpc") && !prop.contains("rss-")) {
        newProps = newProps + ":" + prop;
      }
    }
    System.setProperty("java.class.path", newProps);
    Path newPath = new Path(HDFS_URI + "/rss.jar");
    FileUtil.copy(file, fs, newPath, false, conf);
    DistributedCache.addFileToClassPath(
        new Path(newPath.toUri().getPath()), conf, fs);
    conf.set(MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH,
         "$PWD/rss.jar/" + localFile.getName() + "," + MRJobConfig.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH);
    conf.set("mapreduce.rss.coordinator.quorum", COORDINATOR_QUORUM);
    LargeSorter sorter = new LargeSorter();
    sorter.setConf(conf);
    assertEquals("Large sort failed", 0,
        ToolRunner.run(conf, sorter, new String[0]));
  }

}
