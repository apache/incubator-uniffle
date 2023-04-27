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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.UniffleCliArgsException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExampleCLITest {

  private ExampleCLI exampleCLI;

  @BeforeEach
  public void setup() throws Exception {
    exampleCLI = new ExampleCLI("", "");
  }

  @Test
  public void testHelp() throws UniffleCliArgsException {
    final PrintStream oldOutPrintStream = System.out;
    final PrintStream oldErrPrintStream = System.err;
    ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
    ByteArrayOutputStream dataErr = new ByteArrayOutputStream();
    System.setOut(new PrintStream(dataOut));
    System.setErr(new PrintStream(dataErr));

    String[] args1 = {"-help"};
    assertEquals(0, exampleCLI.run(args1));
    oldOutPrintStream.println(dataOut);
    assertTrue(dataOut.toString().contains(
        "-a,--admin <arg>   This is an example admin command that will print args."));
    assertTrue(dataOut.toString().contains(
        "-c,--cli <arg>     This is an example cli command that will print args."));
    assertTrue(dataOut.toString().contains(
        "-h,--help          Help for the Uniffle Example-CLI."));

    System.setOut(oldOutPrintStream);
    System.setErr(oldErrPrintStream);
    oldOutPrintStream.close();
    oldErrPrintStream.close();
  }

  @Test
  public void testExampleCLI() throws UniffleCliArgsException {
    final PrintStream oldOutPrintStream = System.out;
    final PrintStream oldErrPrintStream = System.err;
    ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
    ByteArrayOutputStream dataErr = new ByteArrayOutputStream();
    System.setOut(new PrintStream(dataOut));
    System.setErr(new PrintStream(dataErr));

    String[] args = {"-c","hello world"};
    assertEquals(0, exampleCLI.run(args));
    oldOutPrintStream.println(dataOut);
    assertTrue(dataOut.toString().contains("example-cli : hello world"));
    System.setOut(oldOutPrintStream);
    System.setErr(oldErrPrintStream);
    oldOutPrintStream.close();
    oldErrPrintStream.close();
  }
}
