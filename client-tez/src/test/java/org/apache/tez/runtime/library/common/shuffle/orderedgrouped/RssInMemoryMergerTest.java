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
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.DataInputBuffer;
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
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.tez.common.RssTezConfig.RSS_REMOTE_SPILL_STORAGE_PATH;
import static org.apache.tez.runtime.library.api.TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS;
import static org.apache.tez.runtime.library.api.TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS;
import static org.apache.tez.runtime.library.common.Constants.TEZ_RUNTIME_TASK_MEMORY;
import static org.apache.tez.runtime.library.common.shuffle.orderedgrouped.RssInMemoryMerger.Counter.NUM_MEM_TO_REMOTE_MERGES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RssInMemoryMergerTest {

  private static final Logger LOG = LoggerFactory.getLogger(RssInMemoryMergerTest.class);

  private static final String[] KEYS = {"aaaaaaaaaa", "bbbbbbbbbb", "cccccccccc", "dddddddddd"};
  private static final String[] VALUES = {"1111111111", "2222222222", "3333333333", "4444444444"};
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
  public void mergerTest() throws Exception {
    // 1 Construct RssMergeManager
    Configuration conf = new Configuration();
    conf.set(RSS_REMOTE_SPILL_STORAGE_PATH, BASE_SPILL_PATH);
    conf.setInt(TEZ_RUNTIME_TASK_MEMORY, 1024);
    conf.setClass(TEZ_RUNTIME_KEY_CLASS, Text.class, Text.class);
    conf.setClass(TEZ_RUNTIME_VALUE_CLASS, Text.class, Text.class);

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

    // 2 Example
    // 2.1 write map outputs
    Map<String, String> map1 = new TreeMap<String, String>();
    map1.put(KEYS[0], VALUES[0]);
    map1.put(KEYS[2], VALUES[2]);
    Map<String, String> map2 = new TreeMap<String, String>();
    map2.put(KEYS[1], VALUES[1]);
    map2.put(KEYS[3], VALUES[3]);
    byte[] mapOutputBytes1 = writeMapOutput(conf, map1);
    ByteArrayInputStream mapInput1 = new ByteArrayInputStream(mapOutputBytes1);
    byte[] mapOutputBytes2 = writeMapOutput(conf, map2);
    ByteArrayInputStream mapInput2 = new ByteArrayInputStream(mapOutputBytes2);
    InputAttemptIdentifier mapId1 = new InputAttemptIdentifier(1, 0);
    InputAttemptIdentifier mapId2 = new InputAttemptIdentifier(2, 0);
    // header lenght is 4, we must substract it!
    MapOutput mapOutput1 =
        MapOutput.createMemoryMapOutput(mapId1, manager, mapOutputBytes1.length - 4, false);
    MapOutput mapOutput2 =
        MapOutput.createMemoryMapOutput(mapId2, manager, mapOutputBytes2.length - 4, false);
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

    // 2.2 Trigger to merge the map outputs
    RssInMemoryMerger merger = manager.getInMemoryMerger();
    List<MapOutput> mapOutputs = new ArrayList();
    mapOutputs.add(mapOutput1);
    mapOutputs.add(mapOutput2);
    merger.merge(mapOutputs);

    // 3 Verify result
    // 3.1 Verify counters
    assertEquals(1, tezCounters.findCounter(NUM_MEM_TO_REMOTE_MERGES).getValue());

    // 3.2 Verify only one remote file contains
    String parentPath = String.format("%s/%s", BASE_SPILL_PATH, APP_ATTEMPT_ID);
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
    Path mergePath = files[0].getPath();

    // 3.3 Verify the content from remote fs
    List<String> keys = Lists.newArrayList();
    List<String> values = Lists.newArrayList();
    readOnDiskMapOutput(remoteFS, mergePath, keys, values);
    remoteFS.delete(new Path(BASE_SPILL_PATH), true);
    for (int i = 0; i < KEYS.length; i++) {
      assertEquals(KEYS[i], keys.get(i));
      assertEquals(VALUES[i], values.get(i));
    }
  }

  static byte[] writeMapOutput(Configuration conf, Map<String, String> keysToValues)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    FSDataOutputStream fsdos = new FSDataOutputStream(baos, null);
    IFile.Writer writer = new IFile.Writer(conf, fsdos, Text.class, Text.class, null, null, null);
    for (String key : keysToValues.keySet()) {
      String value = keysToValues.get(key);
      writer.append(new Text(key), new Text(value));
    }
    writer.close();
    return baos.toByteArray();
  }

  static void readOnDiskMapOutput(FileSystem fs, Path path, List<String> keys, List<String> values)
      throws IOException {
    IFile.Reader reader = new IFile.Reader(fs, path, null, null, null, false, 0, 0);
    DataInputBuffer keyBuff = new DataInputBuffer();
    DataInputBuffer valueBuff = new DataInputBuffer();
    Text key = new Text();
    Text value = new Text();
    while (reader.nextRawKey(keyBuff)) {
      key.readFields(keyBuff);
      keys.add(key.toString());
      reader.nextRawValue(valueBuff);
      value.readFields(valueBuff);
      values.add(value.toString());
    }
  }
}
