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
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CLIContentUtilsTest {

  @Test
  public void testTableFormat() throws IOException, URISyntaxException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter writer = new PrintWriter(baos);
    String titleString = " 5 ShuffleServers were found";
    List<String> headerStrings =
        Arrays.asList(
            "HostName",
            "IP",
            "Port",
            "UsedMem",
            "PreAllocatedMem",
            "AvaliableMem",
            "TotalMem",
            "Status");
    CLIContentUtils formattingCLIUtils = new CLIContentUtils(titleString).addHeaders(headerStrings);
    DecimalFormat df = new DecimalFormat("#.00");
    formattingCLIUtils.addLine(
        "uniffledata-hostname01",
        "10.93.23.11",
        "9909",
        df.format(0.59 * 100) + "%",
        df.format(0.74 * 100) + "%",
        df.format(0.34 * 100) + "%",
        df.format(105) + "G",
        "ACTIVE");
    formattingCLIUtils.addLine(
        "uniffledata-hostname02",
        "10.93.23.12",
        "9909",
        df.format(0.54 * 100) + "%",
        df.format(0.78 * 100) + "%",
        df.format(0.55 * 100) + "%",
        df.format(105) + "G",
        "ACTIVE");
    formattingCLIUtils.addLine(
        "uniffledata-hostname03",
        "10.93.23.13",
        "9909",
        df.format(0.55 * 100) + "%",
        df.format(0.56 * 100) + "%",
        df.format(0.79 * 100) + "%",
        df.format(105) + "G",
        "ACTIVE");
    formattingCLIUtils.addLine(
        "uniffledata-hostname04",
        "10.93.23.14",
        "9909",
        df.format(0.34 * 100) + "%",
        df.format(0.84 * 100) + "%",
        df.format(0.64 * 100) + "%",
        df.format(105) + "G",
        "ACTIVE");
    formattingCLIUtils.addLine(
        "uniffledata-hostname05",
        "10.93.23.15",
        "9909",
        df.format(0.34 * 100) + "%",
        df.format(0.89 * 100) + "%",
        df.format(0.16 * 100) + "%",
        df.format(105) + "G",
        "ACTIVE");
    formattingCLIUtils.addLine(
        "uniffledata-hostname06",
        "10.93.23.16",
        "9909",
        df.format(0.34 * 100) + "%",
        df.format(0.45 * 100) + "%",
        df.format(0.67 * 100) + "%",
        df.format(105) + "G",
        "ACTIVE");
    formattingCLIUtils.addLine(
        "uniffledata-hostname07",
        "10.93.23.17",
        "9909",
        df.format(0.34 * 100) + "%",
        df.format(0.15 * 100) + "%",
        df.format(0.98 * 100) + "%",
        df.format(105) + "G",
        "ACTIVE");
    formattingCLIUtils.addLine(
        "uniffledata-hostname08",
        "10.93.23.18",
        "9909",
        df.format(0.34 * 100) + "%",
        df.format(0.77 * 100) + "%",
        df.format(0.67 * 100) + "%",
        df.format(105) + "G",
        "ACTIVE");
    formattingCLIUtils.addLine(
        "rssdata-hostname09",
        "10.93.23.19",
        "9909",
        df.format(0.14 * 100) + "%",
        df.format(0.44 * 100) + "%",
        df.format(0.68 * 100) + "%",
        df.format(100) + "G",
        "LOST");
    StringBuilder resultStrBuilder = new StringBuilder();
    List<String> lines =
        Files.readAllLines(Paths.get(this.getClass().getResource("/CLIContentResult").toURI()));
    for (String line : lines) {
      if (line != null && line.length() != 0 && !line.startsWith("#")) {
        resultStrBuilder.append(line + "\n");
      }
    }
    String expectStr = resultStrBuilder.toString();
    assertEquals(expectStr, formattingCLIUtils.render());
  }
}
