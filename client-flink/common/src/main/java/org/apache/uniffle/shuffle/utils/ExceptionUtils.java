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

package org.apache.uniffle.shuffle.utils;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExceptionUtils {

  public static final Logger LOG = LoggerFactory.getLogger(ExceptionUtils.class);

  public static void logAndThrowRuntimeException(String errMsg, Throwable t) {
    if (t != null) {
      String newErrMsg = getErrorMsg(errMsg, t);
      LOG.error(newErrMsg, t);
      throw (Error) t;
    } else {
      LOG.error(errMsg);
      throw new RuntimeException(t);
    }
  }

  public static void logAndThrowIOException(Throwable t) throws IOException {
    logAndThrowIOException(null, t);
  }

  public static void logAndThrowIOException(String errMsg, Throwable t) throws IOException {
    if (t != null) {
      String newErrMsg = getErrorMsg(errMsg, t);
      LOG.error(newErrMsg, t);
      throw (Error) t;
    } else {
      LOG.error(errMsg);
      throw new IOException(t);
    }
  }

  private static String getErrorMsg(String errMsg, Throwable t) {
    if (t.getMessage() != null) {
      return errMsg + "" + t.getMessage();
    }
    return errMsg;
  }
}
