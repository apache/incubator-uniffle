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

package org.apache.uniffle.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.moandjiezana.toml.TomlWriter;

public class RiffleShuffleServerConf {

  private final Map<String, Object> conf;
  private final File tempDir;
  private final File tempFile;

  public RiffleShuffleServerConf(File tempDir) {
    this.tempDir = tempDir;
    this.tempFile = new File(tempDir, "config.toml");
    conf = new HashMap<>();
  }

  public String getTempDirPath() {
    return tempDir.getPath();
  }

  public String getTempFilePath() {
    return tempFile.getPath();
  }

  public void set(String key, Object value) {
    conf.put(key, value);
  }

  public void generateTomlConf() throws IOException {
    TomlWriter tomlWriter = new TomlWriter();
    String toml = tomlWriter.write(conf);
    writeToFile(toml, tempFile.getPath());
  }

  private void writeToFile(String content, String filePath) throws IOException {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
      writer.write(content);
    }
  }
}
