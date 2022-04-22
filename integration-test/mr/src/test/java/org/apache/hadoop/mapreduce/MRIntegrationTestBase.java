/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.mapreduce;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.mapreduce.v2.TestMRJobs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.tencent.rss.test.IntegrationTestBase;

public class MRIntegrationTestBase extends IntegrationTestBase {

  protected static MiniMRYarnCluster mrYarnCluster;
  protected static FileSystem localFs;
  static {
    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException io) {
      throw new RuntimeException("problem getting local fs", io);
    }
  }

  private static Path TEST_ROOT_DIR = localFs.makeQualified(
      new Path("target", TestMRJobs.class.getName() + "-tmpDir"));
  static Path APP_JAR = new Path(TEST_ROOT_DIR, "MRAppJar.jar");
  private static final String OUTPUT_ROOT_DIR = "/tmp/" +
      TestMRJobs.class.getSimpleName();
  private static final Path TEST_RESOURCES_DIR = new Path(TEST_ROOT_DIR,
      "localizedResources");

  @BeforeClass
  public static void setUpMRYarn() throws IOException {
    mrYarnCluster = new MiniMRYarnCluster("test");
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, "/apps_staging_dir");
    conf.setInt(YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY, 10);
    mrYarnCluster.init(conf);
    mrYarnCluster.start();
    localFs.copyFromLocalFile(new Path(MiniMRYarnCluster.APPJAR), APP_JAR);
    localFs.setPermission(APP_JAR, new FsPermission("700"));
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (mrYarnCluster != null) {
      mrYarnCluster.stop();
      mrYarnCluster = null;
    }

    if (localFs.exists(TEST_RESOURCES_DIR)) {
      // clean up resource directory
      localFs.delete(TEST_RESOURCES_DIR, true);
    }
  }

  public void run() throws Exception {
    Configuration appConf = new Configuration(mrYarnCluster.getConfig());
    updateCommonConfiguration(appConf);
    runOriginApp(appConf);
    appConf.get("mapreduce.output.fileoutputformat.outputdir");
    appConf = new Configuration(mrYarnCluster.getConfig());
    updateCommonConfiguration(appConf);
    runRssApp(appConf);
    verifyResults();
  }

  private void updateCommonConfiguration(Configuration jobConf) {

  }

  private void runOriginApp(Configuration jobConf) throws Exception {
    runMRApp(jobConf, getTestTool(), getTestArgs());
  }

  private void runRssApp(Configuration jobConf) throws Exception {
    URL url = MRIntegrationTestBase.class.getResource("/");
    String parentPath = new Path(url.getPath()).getParent()
        .getParent().getParent().getParent().toString();
    if (System.getenv("JAVA_HOME") == null) {
      throw new RuntimeException("We must set JAVA_HOME");
    }
    jobConf.set(MRJobConfig.MR_AM_COMMAND_OPTS, "-XX:+TraceClassLoading org.apache.hadoop.mapreduce.v2.app.RssMRAppMaster");
    jobConf.setInt(MRJobConfig.MAP_MEMORY_MB, 2048);
    jobConf.setInt(MRJobConfig.IO_SORT_MB, 128);
    File file = new File(parentPath, "client-mr/target/shaded");
    File[] jars = file.listFiles();
    File localFile = null;
    for (File jar : jars) {
      if (jar.getName().startsWith("rss-client-mr")) {
        localFile = jar;
        break;
      }
    }
    assertNotNull(localFile);
    String props = System.getProperty("java.class.path");
    String newProps = "";
    String[] splittedProps = props.split(":");
    for (String prop : splittedProps)  {
      if (!prop.contains("classes") && !prop.contains("grpc") && !prop.contains("rss-")) {
        newProps = newProps + ":" + prop;
      }
    }
    System.setProperty("java.class.path", newProps);
    Path newPath = new Path(HDFS_URI + "/rss.jar");
    FileUtil.copy(file, fs, newPath, false, jobConf);
    DistributedCache.addFileToClassPath(
        new Path(newPath.toUri().getPath()), jobConf, fs);
    jobConf.set(MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH,
        "$PWD/rss.jar/" + localFile.getName() + "," + MRJobConfig.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH);
    jobConf.set("mapreduce.rss.coordinator.quorum", COORDINATOR_QUORUM);
    updateRssConfiguration(jobConf);
    runMRApp(jobConf, getTestTool(), getTestArgs());

  }

  protected String[] getTestArgs() {
    return new String[0];
  }

  protected void updateRssConfiguration(Configuration jobConf) {

  }

  private void runMRApp(Configuration conf, Tool tool, String[] args) throws Exception {
    assertEquals(tool.getClass().getName() + " failed", 0,
        ToolRunner.run(conf, tool, args));
  }

  protected Tool getTestTool() {
    return null;
  }

  private void verifyResults() {

  }
}
