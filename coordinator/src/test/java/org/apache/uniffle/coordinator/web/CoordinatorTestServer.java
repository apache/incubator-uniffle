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

package org.apache.uniffle.coordinator.web;

import picocli.CommandLine;

import org.apache.uniffle.common.Arguments;
import org.apache.uniffle.common.ReconfigurableConfManager;
import org.apache.uniffle.coordinator.ApplicationManager;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.CoordinatorServer;

/**
 * This class is mainly used to simulate CoordinatorServer. When initializing Coordinator, we will
 * simulate and initialize some other properties, such as Applications.
 */
public class CoordinatorTestServer extends CoordinatorServer {
  public CoordinatorTestServer(CoordinatorConf coordinatorConf) throws Exception {
    super(coordinatorConf);
    generateApplicationList(2000);
  }

  private void generateApplicationList(int count) {
    for (int i = 0; i < count; i++) {
      ApplicationManager applicationManager = getApplicationManager();
      applicationManager.registerApplicationInfo("application_" + i, "test");
    }
  }

  public static void main(String[] args) throws Exception {
    Arguments arguments = new Arguments();
    CommandLine commandLine = new CommandLine(arguments);
    commandLine.parseArgs(args);
    String configFile = arguments.getConfigFile();

    // Load configuration from config files
    final CoordinatorConf coordinatorConf = new CoordinatorConf(configFile);
    ReconfigurableConfManager.init(coordinatorConf, configFile);

    // Start the coordinator service
    final CoordinatorTestServer coordinatorServer = new CoordinatorTestServer(coordinatorConf);

    coordinatorServer.start();
    coordinatorServer.blockUntilShutdown();
  }
}
