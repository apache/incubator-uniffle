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
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RustShuffleServerConf {

    private static final Logger LOG = LoggerFactory.getLogger(RustShuffleServerConf.class);

    private final Map<String, Object> conf;

    private final File tempDir;

    private final File tempFile;

    public RustShuffleServerConf(File tempDir) {
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
        writeToFile(toTomlString(conf), tempFile.getPath());
    }

    public String toTomlString(Map<String, Object> tomlData) {
        StringBuilder tomlBuilder = new StringBuilder();

        for (Map.Entry<String, Object> entry : tomlData.entrySet()) {
            if (!(entry.getValue() instanceof Map)) {
                appendEntry(tomlBuilder, entry);
            }
        }

        for (Map.Entry<String, Object> entry : tomlData.entrySet()) {
            if (entry.getValue() instanceof Map) {
                tomlBuilder.append("\n[").append(entry.getKey()).append("]\n");
                Map<?, ?> nestedMap = (Map<?, ?>) entry.getValue();
                for (Map.Entry<?, ?> nestedEntry : nestedMap.entrySet()) {
                    appendEntry(tomlBuilder, nestedEntry);
                }
            }
        }

        return tomlBuilder.toString();
    }

    private void appendEntry(StringBuilder builder, Map.Entry<?, ?> entry) {
        if (entry.getValue() instanceof List) {
            builder.append(entry.getKey()).append(" = ").append(listToString((List<?>) entry.getValue())).append("\n");
        } else {
            builder.append(entry.getKey()).append(" = ").append(valueToString(entry.getValue())).append("\n");
        }
    }

    private String mapToString(Map<?, ?> map) {
        StringBuilder mapBuilder = new StringBuilder();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            mapBuilder.append(entry.getKey()).append(" = ").append(valueToString(entry.getValue())).append("\n");
        }
        return mapBuilder.toString();
    }

    private String listToString(List<?> list) {
        StringBuilder listBuilder = new StringBuilder("[");
        boolean first = true;
        for (Object item : list) {
            if (!first) {
                listBuilder.append(", ");
            }
            listBuilder.append(valueToString(item));
            first = false;
        }
        listBuilder.append("]");
        return listBuilder.toString();
    }

    private String valueToString(Object value) {
        if (value instanceof String) {
            return "\"" + value + "\"";
        }
        return value.toString();
    }

    private void writeToFile(String content, String filePath) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            writer.write(content);
        }
    }

}
