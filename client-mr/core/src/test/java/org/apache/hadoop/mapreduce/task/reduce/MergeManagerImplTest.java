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

package org.apache.hadoop.mapreduce.task.reduce;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MROutputFiles;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.RssMRUtils;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Assert;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.hadoop.mapreduce.MRJobConfig.REDUCE_MEMORY_TOTAL_BYTES;
import static org.apache.hadoop.mapreduce.MRJobConfig.SHUFFLE_MEMORY_LIMIT_PERCENT;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MergeManagerImplTest {

  private static JobID jobId = new JobID("a", 0);
  private static TaskAttemptID reduceId1 =
      new TaskAttemptID(new TaskID(jobId, TaskType.REDUCE, 0), 0);
  private static LocalDirAllocator lda = new LocalDirAllocator(MRConfig.LOCAL_DIR);
  private static FileSystem fs;

  @ParameterizedTest
  @ValueSource(strings = {"", "org.apache.hadoop.io.compress.BZip2Codec"})
  public void testMerge(String codecClassName) throws Throwable {
    fs = FileSystem.getLocal(new JobConf());
    JobConf jobConf = new JobConf();
    jobConf.setInt(REDUCE_MEMORY_TOTAL_BYTES, 10240);
    jobConf.setFloat(SHUFFLE_MEMORY_LIMIT_PERCENT, 0.1F);
    CompressionCodec codec = getCompressionCodec(jobConf, codecClassName);
    if (codec instanceof BZip2Codec) {
      ((BZip2Codec) codec).setConf(jobConf);
    }
    MergeManagerImpl merger =
        new MergeManagerImpl<Text, Text>(
            reduceId1,
            jobConf,
            fs,
            lda,
            Reporter.NULL,
            codec,
            null,
            null,
            null,
            null,
            null,
            null,
            new Progress(),
            new MROutputFiles());
    int fetcherId = 1;
    int taskId = 1;
    int loops = 5;
    // Add map output in inverted order
    for (int i = loops - 1; i >= 0; i--) {
      TaskAttemptID taskAttemptID =
          RssMRUtils.createMRTaskAttemptId(new JobID(), TaskType.MAP, taskId++, 1);
      byte[] buffer1 = writeMapOutput(jobConf, i * 100, i * 100 + 1);
      MapOutput mapOutput1 = merger.reserve(taskAttemptID, buffer1.length, fetcherId++);
      assertTrue(mapOutput1 instanceof InMemoryMapOutput);
      RssBypassWriter.write(mapOutput1, buffer1, codec);
      mapOutput1.commit();
      taskAttemptID = RssMRUtils.createMRTaskAttemptId(new JobID(), TaskType.MAP, taskId++, 1);
      byte[] buffer2 = writeMapOutput(jobConf, i * 100 + 1, i * 100 + 100);
      MapOutput mapOutput2 = merger.reserve(taskAttemptID, buffer2.length, fetcherId++);
      assertTrue(mapOutput2 instanceof OnDiskMapOutput);
      RssBypassWriter.write(mapOutput2, buffer2, codec);
      mapOutput2.commit();
    }
    RawKeyValueIterator iterator = merger.close();
    SerializationFactory serializationFactory = new SerializationFactory(jobConf);
    Deserializer<Text> deserializer = serializationFactory.getDeserializer(Text.class);
    BytesWritable currentRaw = new BytesWritable();
    DataInputBuffer buffer = new DataInputBuffer();
    deserializer.open(buffer);
    for (int i = 0; i < loops * 100; i++) {
      Assert.assertTrue(iterator.next());
      DataInputBuffer nextBuffer = iterator.getKey();
      currentRaw.set(
          nextBuffer.getData(),
          nextBuffer.getPosition(),
          nextBuffer.getLength() - nextBuffer.getPosition());
      buffer.reset(currentRaw.getBytes(), 0, currentRaw.getLength());
      Text key = new Text();
      key = deserializer.deserialize(key);
      Assert.assertEquals(String.format("k%03d", i), key.toString());

      nextBuffer = iterator.getValue();
      currentRaw.set(
          nextBuffer.getData(),
          nextBuffer.getPosition(),
          nextBuffer.getLength() - nextBuffer.getPosition());
      buffer.reset(currentRaw.getBytes(), 0, currentRaw.getLength());
      Text value = new Text();
      value = deserializer.deserialize(value);
      Assert.assertEquals(String.format("v%03d", i), value.toString());
    }
    Assert.assertFalse(iterator.next());
  }

  private static byte[] writeMapOutput(Configuration conf, int start, int end) throws IOException {
    Map<String, String> keysToValues = new TreeMap<>();
    for (int i = start; i < end; i++) {
      keysToValues.put(String.format("k%03d", i), String.format("v%03d", i));
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    FSDataOutputStream fsdos = new FSDataOutputStream(baos, null);
    IFile.Writer<Text, Text> writer =
        new IFile.Writer<Text, Text>(conf, fsdos, Text.class, Text.class, null, null);
    for (String key : keysToValues.keySet()) {
      String value = keysToValues.get(key);
      writer.append(new Text(key), new Text(value));
    }
    writer.close();
    return baos.toByteArray();
  }

  private static CompressionCodec getCompressionCodec(JobConf jobConf, String name)
      throws ClassNotFoundException {
    if (StringUtils.isBlank(name)) {
      return null;
    }
    Class<? extends CompressionCodec> codecClass =
        jobConf.getClassByName(name).asSubclass(CompressionCodec.class);
    return ReflectionUtils.newInstance(codecClass, jobConf);
  }
}
