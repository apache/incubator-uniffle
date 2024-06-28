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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import com.google.common.primitives.Ints;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.DiskFetchedInput;
import org.apache.tez.runtime.library.common.shuffle.FetchedInputCallback;
import org.apache.tez.runtime.library.common.shuffle.RemoteFetchedInput;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutputFiles;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.common.util.ChecksumUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RssTezBypassWriterTest {

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
  public void testWrite() {
    byte[] data = new byte[] {1, 2, -1, 1, 2, -1, -1};
    MapOutput mapOutput = MapOutput.createMemoryMapOutput(null, null, 7, true);
    RssTezBypassWriter.write(mapOutput, data);
    byte[] r = mapOutput.getMemory();
    assertTrue(Arrays.equals(data, r));

    mapOutput = MapOutput.createMemoryMapOutput(null, null, 8, true);
    r = mapOutput.getMemory();
    assertFalse(Arrays.equals(data, r));
  }

  @Test
  public void testCalcChecksum() throws IOException {
    byte[] data = new byte[] {1, 2, -1, 1, 2, -1, -1};
    byte[] result = new byte[] {-71, -87, 19, -71};
    assertTrue(Arrays.equals(Ints.toByteArray((int) ChecksumUtils.getCrc32(data)), result));
  }

  @Test
  public void testWriteDiskFetchInput(@TempDir File tmpDir) throws IOException {
    byte[] data = new byte[] {1, 2, -1, 1, 2, -1, -1};
    InputAttemptIdentifier inputAttemptIdentifier = new InputAttemptIdentifier(1, 1);
    TezTaskOutputFiles tezTaskOutputFiles = mock(TezTaskOutputFiles.class);
    Path output = new Path(tmpDir.toString(), "out");
    when(tezTaskOutputFiles.getInputFileForWrite(anyInt(), anyInt(), anyLong())).thenReturn(output);
    FetchedInputCallback callback = mock(FetchedInputCallback.class);
    Configuration conf = new Configuration();
    DiskFetchedInput fetchedInput =
        new DiskFetchedInput(
            data.length + 8, inputAttemptIdentifier, callback, conf, null, tezTaskOutputFiles);
    RssTezBypassWriter.write(fetchedInput, data);

    // Verify result
    FileSystem localFS = FileSystem.getLocal(conf).getRaw();
    fetchedInput.commit();
    assertEquals(output, fetchedInput.getInputPath());
    assertTrue(localFS.exists(fetchedInput.getInputPath()));
    assertEquals(data.length + 8, fetchedInput.getSize());
    assertEquals(data.length + 8, localFS.getFileStatus(fetchedInput.getInputPath()).getLen());
    InputStream inputStream = fetchedInput.getInputStream();
    byte[] out = new byte[data.length + 8];
    assertEquals(data.length + 8, inputStream.read(out, 0, data.length + 8));
    // Ignore the first four and last four characters
    for (int i = 4; i < data.length + 4; i++) {
      assertEquals(data[i - 4], out[i]);
    }
    inputStream.close();
    fetchedInput.free();
    assertFalse(localFS.exists(fetchedInput.getInputPath()));
  }

  @Test
  public void testWriteRemoteFetchInput() throws IOException {
    byte[] data = new byte[] {1, 2, -1, 1, 2, -1, -1};
    InputAttemptIdentifier inputAttemptIdentifier = new InputAttemptIdentifier(1, 1);
    FetchedInputCallback callback = mock(FetchedInputCallback.class);
    RemoteFetchedInput fetchedInput =
        new RemoteFetchedInput(
            data.length + 8,
            inputAttemptIdentifier,
            callback,
            remoteFS,
            "/base",
            "uniqueid1",
            "appattemptid1");
    RssTezBypassWriter.write(fetchedInput, data);

    // Verify result
    fetchedInput.commit();
    assertEquals(
        "/base/appattemptid1/uniqueid1_src_1_spill_-1.out", fetchedInput.getInputPath().toString());
    assertTrue(remoteFS.exists(fetchedInput.getInputPath()));
    assertEquals(data.length + 8, fetchedInput.getSize());
    assertEquals(data.length + 8, remoteFS.getFileStatus(fetchedInput.getInputPath()).getLen());
    InputStream inputStream = fetchedInput.getInputStream();
    byte[] out = new byte[data.length + 8];
    assertEquals(data.length + 8, inputStream.read(out, 0, data.length + 8));
    // Ignore the first four and last four characters
    for (int i = 4; i < data.length + 4; i++) {
      assertEquals(data[i - 4], out[i]);
    }
    inputStream.close();
    fetchedInput.free();
    assertFalse(remoteFS.exists(fetchedInput.getInputPath()));
  }
}
