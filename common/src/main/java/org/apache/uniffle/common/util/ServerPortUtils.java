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


public class ServerPortUtils {

  private static final int PORT_RANGE_MIN = 40000;
  private static final int PORT_RANGE_MAX = 65535;
  private static final int PORT_RANGE = PORT_RANGE_MAX - PORT_RANGE_MIN;
  private static final int MAX_ATTEMPTS = 1_000;
  private static final Random random = new Random(System.nanoTime());

  public static int findAvailableTcpPort() {
    int candidatePort;
    int searchCounter = 0;
    do {
      if (searchCounter > MAX_ATTEMPTS) {
        throw new IllegalStateException(String.format(
            "Could not find an available TCP port in the range [%d, %d] after %d attempts",
            PORT_RANGE_MIN, PORT_RANGE_MAX, MAX_ATTEMPTS));
      }
      candidatePort = PORT_RANGE_MIN + random.nextInt(PORT_RANGE + 1);
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
