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

package org.apache.tez.runtime.library.common.sort.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.output.OutputTestHelpers;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.ShuffleServerInfo;

import static org.apache.tez.runtime.library.common.shuffle.impl.RssShuffleManagerTest.APPATTEMPT_ID;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RssUnSorterTest {
  private static Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
  private Configuration conf;
  private FileSystem localFs;
  private Path workingDir;

  /** set up */
  @BeforeEach
  public void setup() throws Exception {
    conf = new Configuration();
    localFs = FileSystem.getLocal(conf);
    workingDir =
        new Path(
                System.getProperty("test.build.data", System.getProperty("java.io.tmpdir", "/tmp")),
                RssSorterTest.class.getName())
            .makeQualified(localFs.getUri(), localFs.getWorkingDirectory());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, Text.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, Text.class.getName());
    conf.set(
        TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS, HashPartitioner.class.getName());
    conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, workingDir.toString());

    Map<String, String> envMap = System.getenv();
    Map<String, String> env = new HashMap<>();
    env.putAll(envMap);
    env.put(
        ApplicationConstants.Environment.CONTAINER_ID.name(),
        "container_e160_1681717153064_3770270_01_000001");

    RssSorterTest.setEnv(env);
  }

  @Test
  public void testCollectAndRecordsPerPartition() throws IOException, InterruptedException {
    TezTaskAttemptID tezTaskAttemptID =
        TezTaskAttemptID.fromString("attempt_1681717153064_3770270_1_00_000000_0");

    OutputContext outputContext = OutputTestHelpers.createOutputContext(conf, workingDir);

    long initialMemoryAvailable = 10240000;
    int shuffleId = 1001;

    RssUnSorter rssSorter =
        new RssUnSorter(
            tezTaskAttemptID,
            outputContext,
            conf,
            5,
            5,
            initialMemoryAvailable,
            shuffleId,
            APPATTEMPT_ID,
            partitionToServers);

    rssSorter.collect(new Text("0"), new Text("0"), 0);
    rssSorter.collect(new Text("0"), new Text("1"), 0);
    rssSorter.collect(new Text("1"), new Text("1"), 1);
    rssSorter.collect(new Text("2"), new Text("2"), 2);
    rssSorter.collect(new Text("3"), new Text("3"), 3);
    rssSorter.collect(new Text("4"), new Text("4"), 4);

    assertTrue(2 == rssSorter.getNumRecordsPerPartition()[0]);
    assertTrue(1 == rssSorter.getNumRecordsPerPartition()[1]);
    assertTrue(1 == rssSorter.getNumRecordsPerPartition()[2]);
    assertTrue(1 == rssSorter.getNumRecordsPerPartition()[3]);
    assertTrue(1 == rssSorter.getNumRecordsPerPartition()[4]);

    assertTrue(5 == rssSorter.getNumRecordsPerPartition().length);
  }
}
