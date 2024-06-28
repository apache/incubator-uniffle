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

package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.runtime.api.impl.TezInputContextImpl;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.tez.common.RssTezConfig.RSS_REMOTE_SPILL_STORAGE_PATH;
import static org.apache.tez.runtime.library.api.TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS;
import static org.apache.tez.runtime.library.api.TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT;
import static org.apache.tez.runtime.library.api.TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT;
import static org.apache.tez.runtime.library.api.TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS;
import static org.apache.tez.runtime.library.common.Constants.TEZ_RUNTIME_TASK_MEMORY;
import static org.apache.tez.runtime.library.common.shuffle.orderedgrouped.RssInMemoryMerger.Counter.NUM_MEM_TO_REMOTE_MERGES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RssMergeManagerTest {

  private static final Logger LOG = LoggerFactory.getLogger(RssMergeManagerTest.class);

  private static final String[] KEYS = {
    "aaaaaaaaaa", "bbbbbbbbbb", "cccccccccc", "dddddddddd", "eeeeeeeeee", "ffffffffff"
  };
  private static final String[] VALUES = {
    "1111111111", "2222222222", "3333333333", "4444444444", "5555555555", "6666666666"
  };
  private static final String BASE_SPILL_PATH = "/tmp";
  private static final ApplicationId APP_ID = ApplicationId.newInstance(1, 1);
  private static final ApplicationAttemptId APP_ATTEMPT_ID =
      ApplicationAttemptId.newInstance(APP_ID, 1);
  private static final String UNIQUE_ID = "TASK_ATTEMPT_1";

  private static FileSystem remoteFS;
  private static MiniDFSCluster cluster;

  @BeforeAll
  public static void setUpHdfs(@TempDir File tempDir) throws Exception {
    Configuration conf = new Configuration();
    File baseDir = tempDir;
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    cluster = (new MiniDFSCluster.Builder(conf)).build();
    String hdfsUri = cluster.getURI().toString() + "/";
    remoteFS = (new Path(hdfsUri)).getFileSystem(conf);
  }

  @AfterAll
  public static void tearDownHdfs() throws Exception {
    remoteFS.close();
    cluster.shutdown();
  }

  @Test
  public void mergerTest() throws Throwable {
    // 1 Construct and start RssMergeManager
    Configuration conf = new Configuration();
    conf.set(RSS_REMOTE_SPILL_STORAGE_PATH, BASE_SPILL_PATH);
    conf.setInt(TEZ_RUNTIME_TASK_MEMORY, 1024);
    conf.setClass(TEZ_RUNTIME_KEY_CLASS, Text.class, Text.class);
    conf.setClass(TEZ_RUNTIME_VALUE_CLASS, Text.class, Text.class);
    conf.setFloat(TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT, 0.07F);
    conf.setFloat(TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT, 0.08F);

    TezInputContextImpl inputContext = mock(TezInputContextImpl.class);
    TezCounters tezCounters = new TezCounters();
    when(inputContext.getCounters()).thenReturn(tezCounters);
    when(inputContext.getSourceVertexName()).thenReturn("vertex0");
    when(inputContext.getUniqueIdentifier()).thenReturn(UNIQUE_ID);
    TezCounter spilledRecordsCounter = tezCounters.findCounter(TaskCounter.SPILLED_RECORDS);
    TezCounter reduceCombineInputCounter =
        tezCounters.findCounter(TaskCounter.COMBINE_INPUT_RECORDS);
    TezCounter mergedMapOutputsCounter = tezCounters.findCounter(TaskCounter.MERGED_MAP_OUTPUTS);

    FileSystem localFS = FileSystem.getLocal(conf);
    int initialMemoryAvailable = 1024;
    CompressionCodec codec = null;
    final RssMergeManager manager =
        new RssMergeManager(
            conf,
            localFS,
            inputContext,
            null,
            spilledRecordsCounter,
            reduceCombineInputCounter,
            mergedMapOutputsCounter,
            null,
            initialMemoryAvailable,
            codec,
            false,
            0,
            remoteFS.getConf(),
            1,
            3,
            APP_ATTEMPT_ID.toString());
    manager.configureAndStart();

    // 2 Example1: Trigger to merge
    // 2.1 write and commit map outputs
    // Total write file is 116, it is larger than 1024 * 0.08, so will trigger to merger.
    Map<String, String> map1 = new TreeMap<String, String>();
    map1.put(KEYS[0], VALUES[0]);
    map1.put(KEYS[2], VALUES[2]);
    Map<String, String> map2 = new TreeMap<String, String>();
    map2.put(KEYS[3], VALUES[3]);
    map2.put(KEYS[5], VALUES[5]);
    byte[] mapOutputBytes1 = RssInMemoryMergerTest.writeMapOutput(conf, map1);
    ByteArrayInputStream mapInput1 = new ByteArrayInputStream(mapOutputBytes1);
    byte[] mapOutputBytes2 = RssInMemoryMergerTest.writeMapOutput(conf, map2);
    ByteArrayInputStream mapInput2 = new ByteArrayInputStream(mapOutputBytes2);
    InputAttemptIdentifier mapId1 = new InputAttemptIdentifier(1, 0);
    InputAttemptIdentifier mapId2 = new InputAttemptIdentifier(2, 0);
    // Header lenght is 4, we must substract it!
    MapOutput mapOutput1 =
        manager.reserve(mapId1, mapOutputBytes1.length - 4, mapOutputBytes1.length - 4, 0);
    MapOutput mapOutput2 =
        manager.reserve(mapId2, mapOutputBytes2.length - 4, mapOutputBytes1.length - 4, 0);
    ShuffleUtils.shuffleToMemory(
        mapOutput1.getMemory(),
        mapInput1,
        mapOutputBytes1.length,
        mapOutputBytes1.length,
        codec,
        false,
        0,
        LOG,
        mapId1);
    ShuffleUtils.shuffleToMemory(
        mapOutput2.getMemory(),
        mapInput2,
        mapOutputBytes2.length,
        mapOutputBytes2.length,
        codec,
        false,
        0,
        LOG,
        mapId2);
    mapOutput1.commit();
    mapOutput2.commit();

    // 2.2 Wait for merge
    manager.waitForInMemoryMerge();

    // 2.3 Verify result
    // 2.3.1 Verify counters
    assertEquals(1, tezCounters.findCounter(NUM_MEM_TO_REMOTE_MERGES).getValue());

    // 2.3.2 Verify only one merged file is located in remote fs
    String parentPath = String.format("%s/%s", BASE_SPILL_PATH, APP_ATTEMPT_ID.toString());
    FileStatus[] files = remoteFS.listStatus(new Path(parentPath));
    assertEquals(1, files.length);
    String filePathPattern =
        String.format(
            "%s/%s/%s_src_(\\d)_spill_%d.out", BASE_SPILL_PATH, APP_ATTEMPT_ID, UNIQUE_ID, -1);
    Pattern pattern = Pattern.compile(filePathPattern);
    Matcher matcher = pattern.matcher(files[0].getPath().toString());
    assertTrue(matcher.find());
    assertTrue(
        matcher.group(1).equals(String.valueOf(mapId1.getInputIdentifier()))
            || matcher.group(1).equals(String.valueOf(mapId2.getInputIdentifier())));

    // 2.3.3 Verify the content from remote fs
    Path mergePath = files[0].getPath();
    List<String> keys = Lists.newArrayList();
    List<String> values = Lists.newArrayList();
    RssInMemoryMergerTest.readOnDiskMapOutput(remoteFS, mergePath, keys, values);
    // assert content in remote fs
    List<String> actualKeys = Lists.newArrayList(KEYS[0], KEYS[2], KEYS[3], KEYS[5]);
    List<String> actualValues = Lists.newArrayList(VALUES[0], VALUES[2], VALUES[3], VALUES[5]);
    for (int i = 0; i < actualValues.size(); i++) {
      assertEquals(keys.get(i), actualKeys.get(i));
      assertEquals(values.get(i), actualValues.get(i));
    }

    // 3 Example2: Wait close to trigger merge
    // 3.1 write and commit map outputs
    // Total write file is 58, it is less than 1024 * 0.08, so will not trigger to merge.
    Map<String, String> map3 = new TreeMap<String, String>();
    map3.put(KEYS[1], VALUES[1]);
    map3.put(KEYS[4], VALUES[4]);
    byte[] mapOutputBytes3 = RssInMemoryMergerTest.writeMapOutput(conf, map3);
    ByteArrayInputStream mapInput3 = new ByteArrayInputStream(mapOutputBytes3);
    // header lenght is 4, we must substract it!
    InputAttemptIdentifier mapId3 = new InputAttemptIdentifier(3, 0);
    MapOutput mapOutput3 =
        manager.reserve(mapId3, mapOutputBytes3.length - 4, mapOutputBytes3.length - 4, 0);
    ShuffleUtils.shuffleToMemory(
        mapOutput3.getMemory(),
        mapInput3,
        mapOutputBytes3.length,
        mapOutputBytes3.length,
        codec,
        false,
        0,
        LOG,
        mapId3);
    mapOutput3.commit();

    // 3.2 Verfiy merge is not trigger, only one remote file exists.
    manager.waitForInMemoryMerge();
    files = remoteFS.listStatus(new Path(parentPath));
    assertEquals(1, files.length);

    // 3.3 Call close, then trigger to mege
    TezRawKeyValueIterator iterator = manager.close(true);

    // 3.4 Verify result
    // 3.4.1 Verify counters
    assertEquals(2, tezCounters.findCounter(NUM_MEM_TO_REMOTE_MERGES).getValue());

    // 3.4.2 Verify two merged file is located in remote fs
    files = remoteFS.listStatus(new Path(parentPath));
    assertEquals(2, files.length);
    String filePath3 =
        String.format(
            "%s/%s/%s_src_%s_spill_%d.out",
            BASE_SPILL_PATH, APP_ATTEMPT_ID, UNIQUE_ID, mapId3.getInputIdentifier(), -1);
    assertTrue(remoteFS.exists(new Path(filePath3)));

    // 3.4.3 Verify the content from remote fs
    mergePath = new Path(filePath3);
    keys = Lists.newArrayList();
    values = Lists.newArrayList();
    RssInMemoryMergerTest.readOnDiskMapOutput(remoteFS, mergePath, keys, values);
    // assert content in remote fs
    actualKeys = Lists.newArrayList(KEYS[1], KEYS[4]);
    actualValues = Lists.newArrayList(VALUES[1], VALUES[4]);
    for (int i = 0; i < actualValues.size(); i++) {
      assertEquals(keys.get(i), actualKeys.get(i));
      assertEquals(values.get(i), actualValues.get(i));
    }

    // 3.4.3 Verify the content from iterator
    for (int i = 0; i < KEYS.length; i++) {
      // test final returned values
      iterator.next();
      byte[] key = new byte[iterator.getKey().getLength()];
      byte[] value = new byte[iterator.getValue().getLength()];
      System.arraycopy(iterator.getKey().getData(), 0, key, 0, key.length);
      System.arraycopy(iterator.getValue().getData(), 0, value, 0, value.length);
      assertEquals(KEYS[i], new Text(key).toString().trim());
      assertEquals(VALUES[i], new Text(value).toString().trim());
    }
  }
}
