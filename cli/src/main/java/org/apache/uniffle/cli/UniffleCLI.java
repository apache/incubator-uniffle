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

public class UniffleCLI extends AbstractCustomCommandLine {

  private static final Logger LOG = LoggerFactory.getLogger(UniffleCLI.class);
  private final Options allOptions;
  private final Option exampleCli;
  private final Option exampleAdmin;
  private final Option help;

  public UniffleCLI(String shortPrefix, String longPrefix) {
    allOptions = new Options();
    exampleCli = new Option(shortPrefix + "c", longPrefix + "cli",
        true, "This is an example cli command that will print args.");
    exampleAdmin = new Option(shortPrefix + "a", longPrefix + "admin",
         true, "This is an example admin command that will print args.");
    help = new Option(shortPrefix + "h", longPrefix + "help",
        false, "Help for the Uniffle Example-CLI.");
    allOptions.addOption(exampleCli);
    allOptions.addOption(exampleAdmin);
    allOptions.addOption(help);
  }

  public int run(String[] args) throws UniffleCliArgsException {
    final CommandLine cmd = parseCommandLineOptions(args, true);

    if (cmd.hasOption(help.getOpt())) {
      printUsage();
      return 0;
    }

    if (cmd.hasOption(exampleCli.getOpt())) {
      String cliArgs = cmd.getOptionValue(exampleCli.getOpt());
      System.out.println("example-cli : " + cliArgs);
      return 0;
    }

    if (cmd.hasOption(exampleAdmin.getOpt())) {
      String cliArgs = cmd.getOptionValue(exampleAdmin.getOpt());
      System.out.println("example-admin : " + cliArgs);
      return 0;
    }

    return 1;
  }

  @Override
  public void addRunOptions(Options baseOptions) {
    baseOptions.addOption(exampleCli);
    baseOptions.addOption(exampleAdmin);
  }

  @Override
  public void addGeneralOptions(Options baseOptions) {
    baseOptions.addOption(help);
  }

  public static void main(String[] args) {
    int retCode;
    try {
      final UniffleCLI cli = new UniffleCLI("", "");
      retCode = cli.run(args);
    } catch (UniffleCliArgsException e) {
      retCode = AbstractCustomCommandLine.handleCliArgsException(e, LOG);
    } catch (Exception e) {
      retCode = AbstractCustomCommandLine.handleError(e, LOG);
    }
    System.exit(retCode);
  }
}
