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

package org.apache.uniffle.server;

import org.apache.hadoop.util.NodeHealthScriptRunner;
import org.apache.hadoop.util.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.exception.RssException;

public class HealthScriptChecker extends Checker {
  private static final Logger LOG = LoggerFactory.getLogger(HealthScriptChecker.class);
  private String healthScriptPath;
  private static final String ERROR_PATTERN = "ERROR";
  private long scriptTimeout;

  HealthScriptChecker(ShuffleServerConf conf) {
    super(conf);
    this.healthScriptPath = conf.getString(ShuffleServerConf.HEALTH_CHECKER_SCRIPT_PATH);
    if (!NodeHealthScriptRunner.shouldRun(healthScriptPath)) {
      LOG.error(
          "Rss health check script:"
              + healthScriptPath
              + " is not available "
              + "or doesn't have execute permission, so abort server.");
      throw new RssException("Health script not available.");
    }
    this.scriptTimeout = conf.getLong(ShuffleServerConf.HEALTH_CHECKER_SCRIPT_EXECUTE_TIMEOUT);
  }

  @Override
  public boolean checkIsHealthy() {
    HealthCheckerExitStatus status = HealthCheckerExitStatus.SUCCESS;
    // always build executor is to load the latest script, the script file can update in need.
    Shell.ShellCommandExecutor commandExecutor =
        new Shell.ShellCommandExecutor(new String[] {healthScriptPath}, null, null, scriptTimeout);
    try {
      commandExecutor.execute();
    } catch (Shell.ExitCodeException e) {
      // ignore the exit code of the script
      status = HealthCheckerExitStatus.FAILED_WITH_EXIT_CODE;
      // On Windows, we will not hit the Stream closed IOException
      // thrown by stdout buffered reader for timeout event.
      if (Shell.WINDOWS && commandExecutor.isTimedOut()) {
        status = HealthCheckerExitStatus.TIMED_OUT;
      }
    } catch (Exception e) {
      LOG.warn("execute health script exception, please check script.", e);
      if (!commandExecutor.isTimedOut()) {
        status = HealthCheckerExitStatus.FAILED_WITH_EXCEPTION;
      } else {
        status = HealthCheckerExitStatus.TIMED_OUT;
      }
    } finally {
      if (status == HealthCheckerExitStatus.SUCCESS) {
        if (hasErrors(commandExecutor.getOutput())) {
          status = HealthCheckerExitStatus.FAILED;
        }
      }
    }
    if (status != HealthCheckerExitStatus.SUCCESS) {
      LOG.warn("health script check failed. exit status : " + status);
    }
    return status == HealthCheckerExitStatus.SUCCESS;
  }

  private boolean hasErrors(String output) {
    String[] splits = output.split("\n");
    for (String split : splits) {
      if (split.startsWith(ERROR_PATTERN)) {
        return true;
      }
    }
    return false;
  }

  private enum HealthCheckerExitStatus {
    SUCCESS,
    TIMED_OUT,
    FAILED_WITH_EXIT_CODE,
    FAILED_WITH_EXCEPTION,
    FAILED
  }
}
