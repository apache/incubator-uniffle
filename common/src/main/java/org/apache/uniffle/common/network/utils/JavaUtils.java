package org.apache.uniffle.common.network.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

public class JavaUtils {
  private static final Logger logger = LoggerFactory.getLogger(JavaUtils.class);

  /**
   * Define a default value for driver memory here since this value is referenced across the code
   * base and nearly all files already use Utils.scala
   */
  public static final long DEFAULT_DRIVER_MEM_MB = 1024;

  /** Closes the given object, ignoring IOExceptions. */
  public static void closeQuietly(Closeable closeable) {
    try {
      if (closeable != null) {
        closeable.close();
      }
    } catch (IOException e) {
      logger.error("IOException should not have been thrown.", e);
    }
  }
}
