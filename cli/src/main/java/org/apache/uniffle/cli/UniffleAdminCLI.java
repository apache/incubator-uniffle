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

package org.apache.uniffle.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.AbstractCustomCommandLine;
import org.apache.uniffle.UniffleCliArgsException;
import org.apache.uniffle.api.AdminRestApi;
import org.apache.uniffle.common.util.http.UniffleRestClient;

public class UniffleAdminCLI extends AbstractCustomCommandLine {

  private static final Logger LOG = LoggerFactory.getLogger(UniffleAdminCLI.class);

  private final Options allOptions;
  private final Option refreshCheckerCli;
  private final Option help;

  public UniffleAdminCLI(String shortPrefix, String longPrefix) {
    allOptions = new Options();

    refreshCheckerCli =
        new Option(
            shortPrefix + "r",
            longPrefix + "refreshChecker",
            false,
            "This is an admin command that will refresh access checker.");
    help =
        new Option(
            shortPrefix + "h", longPrefix + "help", false, "Help for the Uniffle Admin CLI.");

    allOptions.addOption(refreshCheckerCli);
    allOptions.addOption(help);
  }

  public UniffleAdminCLI(String shortPrefix, String longPrefix, UniffleRestClient client) {
    this(shortPrefix, longPrefix);
    this.client = client;
  }

  public int run(String[] args) throws UniffleCliArgsException {
    final CommandLine cmd = parseCommandLineOptions(args, true);

    if (args != null && args.length < 1) {
      printUsage();
      return 1;
    }

    if (cmd.hasOption(help.getOpt())) {
      printUsage();
      return 0;
    }

    if (cmd.hasOption(coordinatorHost.getOpt()) && cmd.hasOption(coordinatorPort.getOpt())) {
      getUniffleRestClient(cmd);
    }

    if (cmd.hasOption(refreshCheckerCli.getOpt())) {
      LOG.info("uniffle-admin-cli : refresh coordinator access checker!");
      refreshAccessChecker();
      return 0;
    }
    return 1;
  }

  private String refreshAccessChecker() throws UniffleCliArgsException {
    if (client == null) {
      throw new UniffleCliArgsException(
          "Missing Coordinator host address and grpc port parameters.");
    }
    AdminRestApi adminRestApi = new AdminRestApi(client);
    return adminRestApi.refreshAccessChecker();
  }

  @Override
  public void addRunOptions(Options baseOptions) {
    for (Object option : allOptions.getOptions()) {
      baseOptions.addOption((Option) option);
    }
  }

  @Override
  public void addGeneralOptions(Options baseOptions) {
    baseOptions.addOption(help);
  }

  public static void main(String[] args) {
    int retCode;
    try {
      final UniffleAdminCLI cli = new UniffleAdminCLI("", "");
      retCode = cli.run(args);
    } catch (UniffleCliArgsException e) {
      retCode = AbstractCustomCommandLine.handleCliArgsException(e, LOG);
    } catch (Exception e) {
      retCode = AbstractCustomCommandLine.handleError(e, LOG);
    }
    System.exit(retCode);
  }
}
