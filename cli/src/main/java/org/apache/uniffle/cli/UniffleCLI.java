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

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.AbstractCustomCommandLine;
import org.apache.uniffle.UniffleCliArgsException;
import org.apache.uniffle.api.AdminRestApi;
import org.apache.uniffle.coordinator.Application;
import org.apache.uniffle.util.FormattingCLIUtils;

public class UniffleCLI extends AbstractCustomCommandLine {

  private static final Logger LOG = LoggerFactory.getLogger(UniffleCLI.class);
  private final Options allOptions;
  private final Option uniffleClientCli;
  private final Option uniffleAdminCli;
  private final Option uniffleApplicationCli;
  private final Option help;

  private static final List<String> APPLICATIONS_HEADER = Arrays.asList(
      "ApplicationId", "User", "Last HeartBeatTime", "RemoteStoragePath");

  public UniffleCLI(String shortPrefix, String longPrefix) {
    allOptions = new Options();
    uniffleClientCli = new Option(shortPrefix + "c", longPrefix + "cli",
        true, "This is an client cli command that will print args.");
    uniffleAdminCli = new Option(shortPrefix + "a", longPrefix + "admin",
        true, "This is an admin command that will print args.");
    uniffleApplicationCli = new Option(shortPrefix + "apps", longPrefix + "applications",
        true, "The command will be used to print a list of applications. \n"
         + "We usually use the command like this: \n"
         + "uniffle -apps|--applications application_167671938823_0001,application_167671938823_0002");
    help = new Option(shortPrefix + "h", longPrefix + "help",
        false, "Help for the Uniffle CLI.");

    allOptions.addOption(uniffleClientCli);
    allOptions.addOption(uniffleAdminCli);
    allOptions.addOption(uniffleApplicationCli);
    allOptions.addOption(coordinatorHost);
    allOptions.addOption(coordinatorPort);
    allOptions.addOption(ssl);
    allOptions.addOption(help);
  }

  public int run(String[] args) throws UniffleCliArgsException, JsonProcessingException {
    final CommandLine cmd = parseCommandLineOptions(args, true);

    if (args.length < 1) {
      printUsage();
      return 1;
    }

    if (cmd.hasOption(help.getOpt())) {
      printUsage();
      return 0;
    }

    if (cmd.hasOption(uniffleClientCli.getOpt())) {
      String cliArgs = cmd.getOptionValue(uniffleClientCli.getOpt());
      System.out.println("uniffle-client-cli : " + cliArgs);
      return 0;
    }

    if (cmd.hasOption(uniffleAdminCli.getOpt())) {
      String cliArgs = cmd.getOptionValue(uniffleAdminCli.getOpt());
      System.out.println("uniffle-admin-cli : " + cliArgs);
      return 0;
    }

    if (cmd.hasOption(coordinatorHost.getOpt()) && cmd.hasOption(coordinatorPort.getOpt())) {
      getUniffleRestClient(cmd);
    }

    if (cmd.hasOption(uniffleApplicationCli.getOpt())) {
      LOG.info("uniffle-client-cli : get applications");
      PrintWriter writer = new PrintWriter(new OutputStreamWriter(
          System.out, Charset.forName(StandardCharsets.UTF_8.name())));
      FormattingCLIUtils formattingCLIUtils = new FormattingCLIUtils("Uniflle Applications")
          .addHeaders(APPLICATIONS_HEADER);
      List<Application> applications = getApplications(cmd);
      if (applications != null) {
        applications.forEach(app -> formattingCLIUtils.addLine(app.getApplicationId(),
            app.getUser(), app.getLastHeartBeatTime(), app.getRemoteStoragePath()));
      }
      writer.print(formattingCLIUtils.render());
      writer.flush();
      return 0;
    }

    return 1;
  }

  @Override
  public void addRunOptions(Options baseOptions) {
    for (Object option : allOptions.getOptions()) {
      baseOptions.addOption((Option) option);
    }
  }

  @Override
  public void addGeneralOptions(Options baseOptions) {
    super.addGeneralOptions(baseOptions);
    baseOptions.addOption(help);
  }

  private List<Application> getApplications(CommandLine cmd)
      throws UniffleCliArgsException, JsonProcessingException {
    if (client == null) {
      throw new UniffleCliArgsException("Missing Coordinator host address and grpc port parameters.");
    }
    AdminRestApi adminRestApi = new AdminRestApi(client);
    String applications = cmd.getOptionValue(uniffleApplicationCli.getOpt()).trim();
    return adminRestApi.getApplications(applications);
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
