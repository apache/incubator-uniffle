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

import com.tencent.rss.test.IntegrationTestBase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.mapreduce.v2.TestMRJobs;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

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
}
