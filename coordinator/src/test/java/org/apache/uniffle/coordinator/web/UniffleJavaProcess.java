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

package org.apache.uniffle.coordinator.web;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

/**
 * Simulate Java process execution for testing purposes. This method can truly simulate the
 * coordinator's rest interface access.
 */
public class UniffleJavaProcess {

  private Process process;
  private static final String JAVA_HOME = "java.home";
  private static final String JAVA_CLASS_PATH = "java.class.path";

  public UniffleJavaProcess(Class<?> clazz, File output) throws IOException, InterruptedException {
    this(clazz, null, output);
  }

  public UniffleJavaProcess(Class<?> clazz, List<String> addClassPaths, File output)
      throws IOException, InterruptedException {
    final String javaHome = System.getProperty(JAVA_HOME);
    final String javaBin = javaHome + File.separator + "bin" + File.separator + "java";

    String classpath = System.getProperty(JAVA_CLASS_PATH);
    classpath = classpath.concat("./src/test/resources");
    if (addClassPaths != null) {
      for (String addClasspath : addClassPaths) {
        classpath = classpath.concat(File.pathSeparatorChar + addClasspath);
      }
    }
    String className = clazz.getCanonicalName();
    System.out.println("className:" + className);
    String coordinator = "./src/test/resources/coordinator.conf";
    ProcessBuilder builder =
        new ProcessBuilder(javaBin, "-cp", classpath, className, "--conf", coordinator);
    builder.redirectInput(ProcessBuilder.Redirect.INHERIT);
    builder.redirectOutput(output);
    builder.redirectError(output);
    process = builder.start();
  }

  public void stop() throws InterruptedException {
    if (process != null) {
      process.destroy();
      process.waitFor();
      process.exitValue();
      // Wait for a while to ensure the port is released
      Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
    }
  }
}
