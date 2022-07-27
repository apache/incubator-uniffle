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

package org.apache.uniffle.client.tools;

import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.client.request.RssDecommissionRequest;
import org.apache.uniffle.client.response.RssDecommissionResponse;
import org.apache.uniffle.common.Arguments;
import org.apache.uniffle.common.config.RssBaseConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import static org.apache.uniffle.common.config.RssBaseConf.RPC_SERVER_PORT;

public class Decommission {
  private static final Logger LOG = LoggerFactory.getLogger(Decommission.class);

  public static void main(String[] args) throws Exception {
    DecommissionArguments arguments = new DecommissionArguments();
    CommandLine commandLine = new CommandLine(arguments);
    commandLine.parseArgs(args);
    String configFile = arguments.getConfigFile();
    RssBaseConf rssBaseConf = new RssBaseConf(configFile);
    int serverPort = rssBaseConf.getInteger(RPC_SERVER_PORT);
    ShuffleServerGrpcClient shuffleServerGrpcClient = new ShuffleServerGrpcClient("127.0.0.1", serverPort);
    RssDecommissionRequest request = new RssDecommissionRequest(arguments.getState());
    RssDecommissionResponse decommission = shuffleServerGrpcClient.decommission(request);
    System.out.println("decommissioned state:" + decommission.isOn());
  }

  private static class DecommissionArguments extends Arguments {
    @CommandLine.Option(names = {"-s", "--state"}, description = "on or off")
    private String state;

    public boolean getState() {
      switch (state) {
        case "on":
          return true;
        case "off":
          return false;
        default:
          throw new RuntimeException(String.format("state [%s] not support", state));
      }
    }
  }
}