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

package org.apache.uniffle;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;

public abstract class AbstractCustomCommandLine implements CustomCommandLine {

  protected void printUsage() {
    System.out.println("Usage:");
    HelpFormatter formatter = new HelpFormatter();
    formatter.setWidth(200);
    formatter.setLeftPadding(5);

    formatter.setSyntaxPrefix("   Optional");
    Options options = new Options();
    addGeneralOptions(options);
    addRunOptions(options);
    formatter.printHelp(" ", options);
  }

  public static int handleCliArgsException(UniffleCliArgsException e, Logger logger) {
    logger.error("Could not parse the command line arguments.", e);

    System.out.println(e.getMessage());
    System.out.println();
    System.out.println("Use the help option (-h or --help) to get help on the command.");
    return 1;
  }

  public static int handleError(Throwable t, Logger logger) {
    logger.error("Error while running the Uniffle Command.", t);

    System.err.println();
    System.err.println("------------------------------------------------------------");
    System.err.println(" The program finished with the following exception:");
    System.err.println();

    t.printStackTrace();
    return 1;
  }

  public static CommandLine parse(Options options, String[] args, boolean stopAtNonOptions)
      throws UniffleCliArgsException {
    final DefaultParser parser = new DefaultParser();

    try {
      return parser.parse(options, args, stopAtNonOptions);
    } catch (ParseException e) {
      throw new UniffleCliArgsException(e.getMessage());
    }
  }

  public CommandLine parseCommandLineOptions(String[] args, boolean stopAtNonOptions)
      throws UniffleCliArgsException {
    final Options options = new Options();
    addGeneralOptions(options);
    addRunOptions(options);
    return parse(options, args, stopAtNonOptions);
  }
}
