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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.gson.Gson;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.AbstractCustomCommandLine;
import org.apache.uniffle.UniffleCliArgsException;
import org.apache.uniffle.api.AdminRestApi;
import org.apache.uniffle.common.Application;

public class UniffleCLI extends AbstractCustomCommandLine {

  private static final Logger LOG = LoggerFactory.getLogger(UniffleCLI.class);
  private static final int EXIT_SUCCESS = 0;
  private static final int EXIT_ERROR = 1;
  private final Options allOptions;
  private final Option uniffleClientCli;
  private final Option uniffleAdminCli;
  private final Option uniffleApplicationCli;
  private final Option uniffleApplicationRegex;
  private final Option uniffleApplicationListCli;
  private final Option uniffleApplicationPageSize;
  private final Option uniffleApplicationCurrentPage;
  private final Option uniffleApplicationHbTimeRange;
  private final Option uniffleOutFormat;
  private final Option uniffleOutPutFile;
  // private final Option uniffleLimit;
  private final Option help;

  private static final List<String> APPLICATIONS_HEADER =
      Arrays.asList(
          "ApplicationId", "User", "Register Time", "Last HeartBeatTime", "RemoteStoragePath");

  public UniffleCLI(String shortPrefix, String longPrefix) {
    allOptions = new Options();

    uniffleClientCli =
        new Option(
            shortPrefix + "c",
            longPrefix + "cli",
            true,
            "This is an client cli command that will print args.");
    uniffleAdminCli =
        new Option(
            shortPrefix + "a",
            longPrefix + "admin",
            true,
            "This is an admin command that will print args.");
    uniffleApplicationCli =
        new Option(
            shortPrefix + "app",
            longPrefix + "applications",
            false,
            "The command will be used to print a list of applications.");
    uniffleApplicationListCli =
        new Option(
            null, longPrefix + "app-list", true, "We can provide an application query list.");
    uniffleApplicationRegex =
        new Option(
            null, longPrefix + "appId-regex", true, "ApplicationId Regex filter expression.");
    uniffleApplicationPageSize =
        new Option(null, longPrefix + "app-pageSize", true, "Application pagination page number");
    uniffleApplicationCurrentPage =
        new Option(
            null, longPrefix + "app-currentPage", true, "Application pagination current page");
    uniffleApplicationHbTimeRange =
        new Option(
            null, longPrefix + "app-heartbeatTimeRange", true, "Application Heartbeat TimeRange");
    uniffleOutFormat =
        new Option(
            shortPrefix + "o",
            longPrefix + "output-format",
            true,
            "We can use the -o|--output-format json option to output application information to json."
                + "We currently only support output in Json format");
    uniffleOutPutFile =
        new Option(
            shortPrefix + "f",
            longPrefix + "file",
            true,
            "We can use the -f|--file to output information to file");
    help = new Option(shortPrefix + "h", longPrefix + "help", false, "Help for the Uniffle CLI.");

    allOptions.addOption(uniffleClientCli);
    allOptions.addOption(uniffleAdminCli);
    allOptions.addOption(uniffleApplicationCli);
    allOptions.addOption(uniffleApplicationListCli);
    allOptions.addOption(uniffleApplicationRegex);
    allOptions.addOption(uniffleApplicationPageSize);
    allOptions.addOption(uniffleApplicationCurrentPage);
    allOptions.addOption(coordinatorHost);
    allOptions.addOption(coordinatorPort);
    allOptions.addOption(uniffleOutFormat);
    allOptions.addOption(uniffleOutPutFile);
    allOptions.addOption(uniffleApplicationHbTimeRange);
    allOptions.addOption(ssl);
    allOptions.addOption(help);
  }

  public int run(String[] args) throws UniffleCliArgsException, JsonProcessingException {
    final CommandLine cmd = parseCommandLineOptions(args, true);

    if (args.length < 1) {
      printUsage();
      return EXIT_ERROR;
    }

    if (cmd.hasOption(help.getOpt())) {
      printUsage();
      return 0;
    }

    if (cmd.hasOption(uniffleClientCli.getOpt())) {
      String cliArgs = cmd.getOptionValue(uniffleClientCli.getOpt());
      System.out.println("uniffle-client-cli : " + cliArgs);
      return EXIT_SUCCESS;
    }

    if (cmd.hasOption(uniffleAdminCli.getOpt())) {
      String cliArgs = cmd.getOptionValue(uniffleAdminCli.getOpt());
      System.out.println("uniffle-admin-cli : " + cliArgs);
      return EXIT_SUCCESS;
    }

    if (cmd.hasOption(coordinatorHost.getOpt()) && cmd.hasOption(coordinatorPort.getOpt())) {
      getUniffleRestClient(cmd);

      // If we use application-cli
      if (cmd.hasOption(uniffleApplicationCli.getLongOpt())) {
        LOG.info("uniffle-client-cli : get applications");

        // If we want to output json file
        if (cmd.hasOption(uniffleOutFormat.getOpt())) {

          // Get the OutFormat.
          String outPutFormat = cmd.getOptionValue(uniffleOutFormat.getOpt()).trim();
          if (StringUtils.isBlank(outPutFormat)) {
            System.out.println("output format is not null.");
            return EXIT_ERROR;
          }

          // We allow users to enter json\Json\JSON etc.
          // If the user enters another format, we will prompt the user that we only support JSON.
          if (!StringUtils.equalsAnyIgnoreCase(outPutFormat, "json")) {
            System.out.println("The output currently supports only JSON format.");
            return EXIT_ERROR;
          }

          String json = getApplicationsJson(cmd);
          if (StringUtils.isBlank(json)) {
            System.out.println("no output result.");
            return EXIT_ERROR;
          }
          System.out.println("application: " + json);

          if (cmd.hasOption(uniffleOutPutFile.getOpt())) {

            // Get output file location.
            String uniffleOutPutFile = cmd.getOptionValue(uniffleOutFormat.getOpt()).trim();
            if (StringUtils.isBlank(uniffleOutPutFile)) {
              System.out.println("The output file cannot be empty.");
              return EXIT_ERROR;
            }

            try (FileOutputStream fos = new FileOutputStream(uniffleOutPutFile);
                OutputStreamWriter osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8)) {
              osw.write(json);
              System.out.println(
                  "applications json data has been written to the file("
                      + uniffleOutPutFile
                      + ").");
            } catch (IOException e) {
              System.out.println(
                  "An error occurred while writing the applications json data to the file("
                      + uniffleOutPutFile
                      + ").");
              return EXIT_ERROR;
            }
          }

          return EXIT_SUCCESS;
        }

        try (PrintWriter writer =
            new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8))) {
          CLIContentUtils formattingCLIUtils =
              new CLIContentUtils("Uniffle Applications").addHeaders(APPLICATIONS_HEADER);
          List<Application> applications = getApplications(cmd);
          if (applications != null) {
            applications.forEach(
                app ->
                    formattingCLIUtils.addLine(
                        app.getApplicationId(),
                        app.getUser(),
                        app.getRegisterTime(),
                        app.getLastHeartBeatTime(),
                        app.getRemoteStoragePath()));
          }
          writer.print(formattingCLIUtils.render());
          writer.flush();
          return EXIT_SUCCESS;
        }
      }
    }

    return EXIT_ERROR;
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

  /**
   * Get application list.
   *
   * @param cmd command.
   * @return application list.
   * @throws UniffleCliArgsException an exception that indicates an error or issue related to
   *     command-line arguments in the Uniffle program.
   * @throws JsonProcessingException Intermediate base class for all problems encountered when
   *     processing (parsing, generating) JSON content that are not pure I/O problems.
   */
  private List<Application> getApplications(CommandLine cmd)
      throws UniffleCliArgsException, JsonProcessingException {
    if (client == null) {
      throw new UniffleCliArgsException(
          "Missing Coordinator host address and grpc port parameters.");
    }

    // Condition 1: uniffleApplicationListCli
    String applications = null;
    if (cmd.hasOption(uniffleApplicationListCli.getLongOpt())) {
      applications = cmd.getOptionValue(uniffleApplicationListCli.getLongOpt()).trim();
    }

    // Condition 2: uniffleApplicationRegex
    String applicationIdRegex = null;
    if (cmd.hasOption(uniffleApplicationRegex.getLongOpt())) {
      applicationIdRegex = cmd.getOptionValue(uniffleApplicationRegex.getLongOpt()).trim();
    }

    // Condition 3: pageSize
    String pageSize = null;
    if (cmd.hasOption(uniffleApplicationPageSize.getLongOpt())) {
      pageSize = cmd.getOptionValue(uniffleApplicationPageSize.getLongOpt()).trim();
    }

    // Condition 4: currentPage
    String currentPage = null;
    if (cmd.hasOption(uniffleApplicationCurrentPage.getLongOpt())) {
      currentPage = cmd.getOptionValue(uniffleApplicationCurrentPage.getLongOpt()).trim();
    }

    // Condition 5: heartBeatStartTime
    String heartBeatTimeRange = null;
    if (cmd.hasOption(uniffleApplicationHbTimeRange.getLongOpt())) {
      heartBeatTimeRange = cmd.getOptionValue(uniffleApplicationHbTimeRange.getLongOpt()).trim();
    }

    AdminRestApi adminRestApi = new AdminRestApi(client);
    return adminRestApi.getApplications(
        applications, applicationIdRegex, pageSize, currentPage, heartBeatTimeRange);
  }

  /**
   * Get application Json.
   *
   * @param cmd command.
   * @return application Json.
   * @throws UniffleCliArgsException an exception that indicates an error or issue related to
   *     command-line arguments in the Uniffle program.
   * @throws JsonProcessingException Intermediate base class for all problems encountered when
   *     processing (parsing, generating) JSON content that are not pure I/O problems.
   */
  private String getApplicationsJson(CommandLine cmd)
      throws UniffleCliArgsException, JsonProcessingException {
    List<Application> applications = getApplications(cmd);
    Gson gson = new Gson();
    return gson.toJson(applications);
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
