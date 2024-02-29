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

package org.apache.uniffle.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.opentest4j.AssertionFailedError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RiffleShuffleServer {

  private static final Logger LOG = LoggerFactory.getLogger(RiffleShuffleServer.class);

  private static final String PROJECT_ROOT = System.getProperty("user.dir");

  private final RiffleShuffleServerConf riffleShuffleServerConf;

  private final ExecutorService executorService;

  private Process rustServer;

  public RiffleShuffleServer(RiffleShuffleServerConf riffleShuffleServerConf) {
    this.riffleShuffleServerConf = riffleShuffleServerConf;

    int corePoolSize = 10;
    int maximumPoolSize = 20;
    long keepAliveTime = 60;
    int queueCapacity = 100;

    this.executorService =
        new ThreadPoolExecutor(
            corePoolSize,
            maximumPoolSize,
            keepAliveTime,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(queueCapacity),
            new ThreadPoolExecutor.CallerRunsPolicy());
  }

  public static void compileRustServer() throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder("cargo", "build");
    LOG.info("Project root is: {}", PROJECT_ROOT);

    builder.directory(new File(PROJECT_ROOT + "/../../rust/experimental/server"));

    // Redirect error stream to standard output stream
    builder.redirectErrorStream(true);

    Process process = builder.start();

    LOG.info("Started to compile rust server");

    // Read output (and error) stream of the process
    try (InputStreamReader isr = new InputStreamReader(process.getInputStream());
        BufferedReader br = new BufferedReader(isr)) {

      String line;
      while ((line = br.readLine()) != null) {
        LOG.info(line);
      }
    }

    int exitCode = process.waitFor();
    if (exitCode != 0) {
      LOG.error("Compilation error with exit code: " + exitCode);
      throw new RuntimeException("Failed to compile rust server, exit code: " + exitCode);
    } else {
      LOG.info("Complete compile rust server");
    }
  }

  public void start() throws IOException, InterruptedException, RuntimeException {
    String[] command = {
      PROJECT_ROOT + "/../../rust/experimental/server/target/debug/uniffle-worker",
      "--config",
      riffleShuffleServerConf.getTempFilePath()
    };
    rustServer = Runtime.getRuntime().exec(command);

    Thread.sleep(1000);

    // Create a task to read the standard output of the Rust server
    executorService.submit(
        () -> {
          try (BufferedReader stdInput =
              new BufferedReader(new InputStreamReader(rustServer.getInputStream()))) {
            String line;
            while ((line = stdInput.readLine()) != null) {
              System.out.println(line);
            }
          } catch (IOException e) {
            LOG.error("IOException occurred while reading the input stream", e);
          }
        });

    // Create a task to read the error output of the Rust server
    executorService.submit(
        () -> {
          try (BufferedReader stdError =
              new BufferedReader(new InputStreamReader(rustServer.getErrorStream()))) {
            String line;
            StringBuilder errorMessage = new StringBuilder();
            while ((line = stdError.readLine()) != null) {
              System.out.println(line);
              errorMessage.append("\n");
              errorMessage.append(line);
            }
            assertEquals(errorMessage.toString(), "");
          } catch (IOException e) {
            LOG.error("IOException occurred while reading the error stream", e);
          } catch (AssertionFailedError e) {
            shutdown();
            throw new RuntimeException("Server occurs error", e);
          }
        });

    // Create a task to wait for the end of the Rust server
    executorService.submit(
        () -> {
          try {
            int exitCode = rustServer.waitFor();
            if (exitCode == 0) {
              LOG.info("Rust server stopped successfully.");
            } else {
              LOG.error("Rust server exited with error code: " + exitCode);
            }
          } catch (InterruptedException e) {
            shutdown();
            LOG.error("Interrupted while waiting for the rust server process", e);
          }
        });
  }

  public void stopServer() throws NoSuchFieldException, IllegalAccessException, IOException {
    if (rustServer != null) {
      String os = System.getProperty("os.name").toLowerCase();
      String command;
      if (os.contains("win")) {
        // Windows
        command = "taskkill /PID " + getPid() + " /F";
      } else {
        // Linux and Mac
        command = "kill -15 " + getPid();
      }

      Runtime.getRuntime().exec(command);
    }
  }

  private int getPid() throws NoSuchFieldException, IllegalAccessException {
    Field field = rustServer.getClass().getDeclaredField("pid");
    field.setAccessible(true);
    return field.getInt(rustServer);
  }

  private void shutdown() {
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
    }
  }
}
