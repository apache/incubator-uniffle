package org.apache.uniffle.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RustShuffleServerConf {

    private static final Logger LOG = LoggerFactory.getLogger(RustShuffleServerConf.class);

    private final File tempDir;

    private final File tempFile;

    public RustShuffleServerConf(File tempDir) {
        this.tempDir = tempDir;
        this.tempFile = new File(tempDir, "config.toml");
    }

    public String getTempDirPath() {
        return tempDir.getPath();
    }

    public String getTempFilePath() {
        return tempFile.getPath();
    }

    public void generateTomlConf(Map<String, Object> tomlData) throws IOException {
        writeToFile(toTomlString(tomlData), tempFile.getPath());
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
