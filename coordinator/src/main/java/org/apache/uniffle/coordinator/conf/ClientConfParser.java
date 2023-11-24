package org.apache.uniffle.coordinator.conf;

import java.io.InputStream;

public interface ClientConfParser {
  enum Parser {
    YAML,
    LEGACY,
    MIXED
  }

  ClientConf tryParse(InputStream fileInputStream) throws Exception;
}
