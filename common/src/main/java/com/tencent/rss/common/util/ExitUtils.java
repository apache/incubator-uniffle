/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available. 
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.rss.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExitUtils {

  private static boolean isSystemExitDisabled = false;

  public static class ExitException extends RuntimeException {

    private final int status;

    ExitException(int status, String message, Throwable throwable) {
      super(message, throwable);
      this.status = status;
    }

    public int getStatus() {
      return status;
    }
  }

  /**
   *
   * @param status  exit status
   * @param message terminate message
   * @param throwable throwable caused terminate
   * @param logger  logger of the caller
   */
  public static void terminate(int status, String message, Throwable throwable, Logger logger) throws ExitException {
    if (logger != null) {
      final String s = "Terminating with exit status " + status + ": " + message;
      if (status == 0) {
        logger.info(s, throwable);
      } else {
        logger.error(s, throwable);
      }
    }

    if (!isSystemExitDisabled) {
      System.exit(status);
    }

    throw new ExitException(status, message, throwable);
  }

  public static void disableSystemExit() {
    isSystemExitDisabled = true;
  }

}
