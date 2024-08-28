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

package org.apache.uniffle.common.port;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for reserving ports during tests.
 *
 * <p>The registry reserves ports by taking file locks on files in the port coordination directory.
 * This doesn't prevent external processes from stealing our ports, but it will prevent us from
 * conflicting with ourselves. We can then run tests in a dockerized environment to completely
 * prevent conflicts.
 *
 * <p>The default coordination directory is determined by the "user.dir" jvm property. The
 * coordination directory can be overridden by setting the UNIFFLE_PORT_COORDINATION_DIR environment
 * variable.
 */
public final class PortRegistry {
  private static final String PORT_COORDINATION_DIR_PROPERTY = "UNIFFLE_PORT_COORDINATION_DIR";

  private static final Registry INSTANCE = new Registry();

  private PortRegistry() {} // Class should not be instantiated.

  /**
   * Reserves a free port so that other tests will not take it.
   *
   * @return the free port
   */
  public static int reservePort() {
    return INSTANCE.reservePort();
  }

  /** @param port the port to release */
  public static void release(int port) {
    INSTANCE.release(port);
  }

  /** Clears the registry. */
  public static void clear() {
    INSTANCE.clear();
  }

  /**
   * @return a port that is currently free. This does not reserve the port, so the port may be taken
   *     by the time this method returns.
   */
  public static int getFreePort() {
    int port;
    try {
      ServerSocket socket = new ServerSocket(0);
      port = socket.getLocalPort();
      socket.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return port;
  }

  private static class Registry {
    // Map from port number to the reservation for that port.
    private final Map<Integer, Reservation> reserved = new ConcurrentHashMap<>();
    private final File coordinationDir;

    private Registry() {
      String dir = System.getenv(PORT_COORDINATION_DIR_PROPERTY);
      if (dir == null) {
        dir = System.getProperty("user.dir");
      }
      coordinationDir = new File(dir, ".port_coordination");
      coordinationDir.mkdirs();
    }

    /**
     * Reserves a free port so that other tests will not take it.
     *
     * @return the free port
     */
    public int reservePort() {
      for (int i = 0; i < 1000; i++) {
        int port = getFreePort();
        if (lockPort(port)) {
          return port;
        }
      }
      throw new RuntimeException("Failed to acquire port");
    }

    /**
     * Attempts to lock the given port.
     *
     * @param port the port to lock
     * @return whether the locking succeeded
     */
    public boolean lockPort(int port) {
      File portFile = portFile(port);
      try {
        FileChannel channel = new RandomAccessFile(portFile, "rw").getChannel();
        FileLock lock = channel.tryLock();
        if (lock == null) {
          channel.close();
          return false;
        }
        reserved.put(port, new Reservation(portFile, lock));
        return true;
      } catch (IOException | OverlappingFileLockException e) {
        return false;
      }
    }

    /** @param port the port to release */
    public void release(int port) {
      Reservation r = reserved.remove(port);
      if (r != null) {
        // If delete fails, we may leave a file behind. However, the file will be unlocked, so
        // another process can still take the port.
        r.file.delete();
        try {
          r.lock.release();
          r.lock.channel().close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    /** Clears the registry. */
    public void clear() {
      new HashSet<>(reserved.keySet()).forEach(this::release);
    }

    /**
     * Creates a file in coordination dir to lock the port.
     *
     * @param port the port to lock
     * @return the created file
     */
    public File portFile(int port) {
      return new File(coordinationDir, Integer.toString(port));
    }

    /** Resources used to reserve a port. */
    private static class Reservation {
      private final File file;
      private final FileLock lock;

      private Reservation(File file, FileLock lock) {
        this.file = file;
        this.lock = lock;
      }
    }
  }
}
