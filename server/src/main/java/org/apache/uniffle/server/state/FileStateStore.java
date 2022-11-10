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

package org.apache.uniffle.server.state;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import javax.annotation.Nonnull;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.lang3.StringUtils;

import org.apache.uniffle.common.exception.RssException;

public class FileStateStore implements StateStore {

  // todo: Support storing state to remote filesystem, like HDFS/S3 and so on.
  private String stateLocationPath;

  public FileStateStore(@Nonnull String stateLocationPath) {
    if (StringUtils.isEmpty(stateLocationPath)) {
      throw new RssException("State store location should be not empty");
    }
    this.stateLocationPath = stateLocationPath;
  }

  public void export(ShuffleServerState state) throws Exception {
    Output output = new Output(new FileOutputStream(stateLocationPath));
    // todo: Extract the serialization method as a general interface to support more serialization mode.
    new Kryo().writeObject(output, state);
    output.close();
  }

  public ShuffleServerState restore() throws Exception {
    Input input = new Input(new FileInputStream(stateLocationPath));
    ShuffleServerState state = new Kryo().readObject(input, ShuffleServerState.class);
    return state;
  }
}
