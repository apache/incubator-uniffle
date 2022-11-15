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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.exception.RssException;

public class FileStateStore implements StateStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileStateStore.class);

  // todo: Support storing state to remote filesystem, like HDFS/S3 and so on.
  private String stateLocationPath;

  private Kryo kryo;

  public FileStateStore(@Nonnull String stateLocationPath) {
    if (StringUtils.isEmpty(stateLocationPath)) {
      throw new RssException("State store location should be not empty");
    }
    this.stateLocationPath = stateLocationPath;
    this.kryo = new Kryo();
    kryo.register(TreeRangeMap.class, new RangeMapSerializer());
  }

  public void export(ShuffleServerState state) throws Exception {
    Output output = new Output(new FileOutputStream(stateLocationPath));
    // todo: Extract the serialization method as a general interface to support more serialization mode.
    kryo.writeObject(output, state);
    output.close();
  }

  public ShuffleServerState restore() throws Exception {
    Input input = new Input(new FileInputStream(stateLocationPath));
    ShuffleServerState state = kryo.readObject(input, ShuffleServerState.class);
    input.close();

    File stateFile = new File(stateLocationPath);
    stateFile.delete();
    LOGGER.info("The state file has been deleted, path: {}", stateLocationPath);

    return state;
  }

  static class Range<T> {
    private T low;
    private T upper;

    Range() {
      // ignore
    }

    Range(T low, T upper) {
      this.low = low;
      this.upper = upper;
    }

    public T getLow() {
      return low;
    }

    public void setLow(T low) {
      this.low = low;
    }

    public T getUpper() {
      return upper;
    }

    public void setUpper(T upper) {
      this.upper = upper;
    }
  }

  class RangeMapSerializer<T1 extends Comparable, T2> extends Serializer<RangeMap<T1, T2>> {

    @Override
    public void write(Kryo kryo, Output output, RangeMap<T1, T2> rangeMap) {
      HashMap<Range<T1>, T2> map = new HashMap<>();
      for (Map.Entry<com.google.common.collect.Range<T1>, T2> entry : rangeMap.asMapOfRanges().entrySet()) {
        map.put(
            new Range<>(entry.getKey().lowerEndpoint(), entry.getKey().upperEndpoint()),
            entry.getValue()
        );
      }
      kryo.writeObject(output, map);
    }

    @Override
    public RangeMap<T1, T2> read(Kryo kryo, Input input, Class<RangeMap<T1, T2>> rangeMapClass) {
      HashMap<Range<T1>, T2> map = kryo.readObject(input, HashMap.class);
      RangeMap<T1, T2> rangeMap = TreeRangeMap.create();
      map.entrySet().stream().forEach(x -> {
        rangeMap.put(com.google.common.collect.Range.closed(x.getKey().getLow(), x.getKey().getUpper()), x.getValue());
      });
      return rangeMap;
    }
  }
}
