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

package org.apache.uniffle.common.util;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Random;
import javax.net.ServerSocketFactory;

import org.apache.uniffle.common.config.RssBaseConf;

import static org.apache.uniffle.common.config.RssBaseConf.RSS_RANDOM_PORT_ATTEMPT;
import static org.apache.uniffle.common.config.RssBaseConf.RSS_RANDOM_PORT_MAX;
import static org.apache.uniffle.common.config.RssBaseConf.RSS_RANDOM_PORT_MIN;

public class ServerPortUtils {

  public static int findAvailableTcpPort(RssBaseConf baseConf) {
    int portRangeMin = baseConf.getInteger(RSS_RANDOM_PORT_MIN);
    int portRangeMax =  baseConf.getInteger(RSS_RANDOM_PORT_MAX);
    int portRange = portRangeMax - portRangeMin;
    int maxAttempts = baseConf.getInteger(RSS_RANDOM_PORT_ATTEMPT);
    Random random = new Random(System.nanoTime());
    int candidatePort;
    int searchCounter = 0;
    do {
      if (searchCounter > maxAttempts) {
        throw new IllegalStateException(String.format(
            "Could not find an available TCP port in the range [%d, %d] after %d attempts",
            portRangeMin, portRangeMax, maxAttempts));
      }
      candidatePort = portRangeMin + random.nextInt(portRange + 1);
      searchCounter++;
    } while (!isPortAvailable(candidatePort));

    return candidatePort;
  }

  /**
   * Determine if the specified TCP port is currently available on {@code localhost}.
   */
  public static boolean isPortAvailable(int port) {
    try {
      ServerSocket serverSocket = ServerSocketFactory.getDefault().createServerSocket(
          port, 1, InetAddress.getByName("localhost"));
      serverSocket.close();
      return true;
    } catch (Exception ex) {
      return false;
    }
  }

}
