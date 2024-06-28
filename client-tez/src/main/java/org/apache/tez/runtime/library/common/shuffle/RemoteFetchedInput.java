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

package org.apache.tez.runtime.library.common.shuffle;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteFetchedInput extends FetchedInput {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteFetchedInput.class);

  /*
   * Output path must be unique, is composed of unique_id, src_id and spill_id.
   * unique_id is task attempt id + io context increasing id.
   * src_id the source input id, is determined by partition and source task.
   * spill_id is the spill id of map task.
   * */
  private static final String OUTPUT_FILE_PATTERN = "%s/%s_src_%d_spill_%d.out";

  private final FileSystem remoteFS;
  private final Path tmpOutputPath;
  private final Path outputPath;
  private final long size;

  public RemoteFetchedInput(
      long compressedSize,
      InputAttemptIdentifier inputAttemptIdentifier,
      FetchedInputCallback callbackHandler,
      FileSystem remoteFS,
      String remoteSpillBasePath,
      String uniqueId,
      String appAttemptId) {
    super(inputAttemptIdentifier, callbackHandler);
    this.size = compressedSize;
    this.remoteFS = remoteFS;
    String outputFile =
        String.format(
            OUTPUT_FILE_PATTERN,
            appAttemptId,
            uniqueId,
            this.getInputAttemptIdentifier().getInputIdentifier(),
            this.getInputAttemptIdentifier().getSpillEventId());
    this.outputPath = new Path(remoteSpillBasePath, outputFile);
    // Files are not clobbered due to the id being appended to the outputPath in the tmpPath,
    // otherwise fetches for the same task but from different attempts would clobber each other.
    this.tmpOutputPath = outputPath.suffix(String.valueOf(getId()));
  }

  @Override
  public Type getType() {
    return Type.DISK;
  }

  @Override
  public long getSize() {
    return size;
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    return remoteFS.create(tmpOutputPath);
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return remoteFS.open(outputPath);
  }

  public final Path getInputPath() {
    if (isState(State.COMMITTED)) {
      return this.outputPath;
    }
    return this.tmpOutputPath;
  }

  @Override
  public void commit() throws IOException {
    if (isState(State.PENDING)) {
      setState(State.COMMITTED);
      remoteFS.rename(tmpOutputPath, outputPath);
      notifyFetchComplete();
    }
  }

  @Override
  public void abort() throws IOException {
    if (isState(State.PENDING)) {
      setState(State.ABORTED);
      // TODO NEWTEZ Maybe defer this to container cleanup
      remoteFS.delete(tmpOutputPath, false);
      notifyFetchFailure();
    }
  }

  @Override
  public void free() {
    Preconditions.checkState(
        isState(State.COMMITTED) || isState(State.ABORTED),
        "FetchedInput can only be freed after it is committed or aborted");
    if (isState(State.COMMITTED)) {
      setState(State.FREED);
      try {
        // TODO NEWTEZ Maybe defer this to container cleanup
        remoteFS.delete(outputPath, false);
      } catch (IOException e) {
        // Ignoring the exception, will eventually be cleaned by container
        // cleanup.
        LOG.warn("Failed to remvoe file : " + outputPath.toString());
      }
      notifyFreedResource();
    }
  }

  @Override
  public String toString() {
    return "RemoteFetchedInput [outputPath="
        + outputPath
        + ", inputAttemptIdentifier="
        + getInputAttemptIdentifier()
        + ", actualSize="
        + getSize()
        + ", type="
        + getType()
        + ", id="
        + getId()
        + ", state="
        + getState()
        + "]";
  }
}
