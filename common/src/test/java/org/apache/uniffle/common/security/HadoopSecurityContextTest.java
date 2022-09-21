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

package org.apache.uniffle.common.security;

import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.KerberizedHdfsBase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class HadoopSecurityContextTest extends KerberizedHdfsBase {

  @BeforeAll
  public static void beforeAll() throws Exception {
    testRunner = HadoopSecurityContextTest.class;
    KerberizedHdfsBase.init();
  }

  @Test
  public void testSecuredCallable() throws Exception {
    HadoopSecurityContext context = new HadoopSecurityContext(
        null,
        kerberizedHdfs.getHdfsKeytab(),
        kerberizedHdfs.getHdfsPrincipal(),
        1000
    );

    // case1: when user is empty or null, it should throw exception
    try {
      context.runSecured(StringUtils.EMPTY, (Callable<Void>) () -> null);
      fail();
    } catch (Exception e) {
      return;
    }

    // case2: run by the login user, there is no need to wrap proxy action
    Path pathWithHdfsUser = new Path("/hdfs/HadoopSecurityContextTest");
    context.runSecured("hdfs", (Callable<Void>) () -> {
      kerberizedHdfs.getFileSystem().mkdirs(pathWithHdfsUser);
      return null;
    });
    FileStatus fileStatus = kerberizedHdfs.getFileSystem().getFileStatus(pathWithHdfsUser);
    assertEquals("hdfs", fileStatus.getOwner());

    // case3: run by the proxy user
    Path pathWithAlexUser = new Path("/alex/HadoopSecurityContextTest");
    context.runSecured("alex", (Callable<Void>) () -> {
      kerberizedHdfs.getFileSystem().mkdirs(pathWithAlexUser);
      return null;
    });
    fileStatus = kerberizedHdfs.getFileSystem().getFileStatus(pathWithAlexUser);
    assertEquals("alex", fileStatus.getOwner());

    context.close();
  }

  @Test
  public void testCreateIllegalContext() throws Exception {
    // case1: lack principal, should throw exception
    try {
      HadoopSecurityContext context = new HadoopSecurityContext(
          null,
          kerberizedHdfs.getHdfsKeytab(),
          null,
          1000
      );
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("principal must be not null or empty"));
    }

    // case2: lack keytab, should throw exception
    try {
      HadoopSecurityContext context = new HadoopSecurityContext(
          null,
          null,
          kerberizedHdfs.getHdfsPrincipal(),
          1000
      );
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("KeytabFilePath must be not null or empty"));
    }

    // case3: illegal relogin interval sec
    try {
      HadoopSecurityContext context = new HadoopSecurityContext(
          null,
          kerberizedHdfs.getHdfsKeytab(),
          kerberizedHdfs.getHdfsPrincipal(),
          0
      );
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("refreshIntervalSec must be not negative"));
    }

    // case4: lack krb5 conf, should throw exception
    String krbConfFilePath = System.getProperty("java.security.krb5.conf");
    System.clearProperty("java.security.krb5.conf");
    try {
      HadoopSecurityContext context = new HadoopSecurityContext(
          null,
          kerberizedHdfs.getHdfsKeytab(),
          kerberizedHdfs.getHdfsPrincipal(),
          100
      );
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Cannot locate KDC"));
    }

    // case5: After setting the krb5 conf, it should pass
    HadoopSecurityContext context = new HadoopSecurityContext(
        krbConfFilePath,
        kerberizedHdfs.getHdfsKeytab(),
        kerberizedHdfs.getHdfsPrincipal(),
        100
    );

    // recover System property of krb5 conf
    System.setProperty("java.security.krb5.conf", krbConfFilePath);
  }
}
