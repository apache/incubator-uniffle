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

package org.apache.hadoop.mapreduce.v2.app;

import java.io.File;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RssMRAppMasterTest {

  @Test
  public void testUpdateConf() {
    JobConf jobConf = new JobConf();
    Path path = new Path("test");
    assertNull(jobConf.get("A"));
    jobConf.set("A", "B");
    RssMRAppMaster.updateConf(jobConf, path);
    File file = new File(MRJobConfig.JOB_CONF_FILE);
    assertTrue(file.exists());
    JobConf newJobConf = new JobConf(new Path(MRJobConfig.JOB_CONF_FILE));
    assertEquals("B", newJobConf.get("A"));
    file.deleteOnExit();
  }
}
