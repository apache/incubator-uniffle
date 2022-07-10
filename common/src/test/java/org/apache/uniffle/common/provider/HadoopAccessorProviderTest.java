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

package org.apache.uniffle.common.provider;

import java.io.BufferedWriter;
import java.io.File;
import java.io.OutputStreamWriter;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class HadoopAccessorProviderTest extends KerberizedHdfsTestBase {

  @BeforeAll
  public static void setup() throws Exception {
    KerberizedHdfsTestBase.setup();
  }

  @AfterAll
  public static void afterAll() throws Exception {
    KerberizedHdfsTestBase.tearDown();
  }

  @Test
  public void testIllegalInitialization() throws Exception {
    // case1: nothing in securityInfo
    try {
      SecurityInfo securityInfo = SecurityInfo.newBuilder().build();
      HadoopAccessorProvider.init(securityInfo);
      fail();
    } catch (Exception e) {
      // ingore.
    } finally {
      HadoopAccessorProvider.cleanup();
    }

    // case2: lack configs in securityInfo
    try {
      SecurityInfo securityInfo = SecurityInfo.newBuilder()
          .keytabFilePath("xxx")
          .build();
      HadoopAccessorProvider.init(securityInfo);
      fail();
    } catch (Exception e) {
      // ingore.
    } finally {
      HadoopAccessorProvider.cleanup();
    }
  }

  @Test
  public void testIllegallyGetFsByIncorrectParams() throws Exception {
    // case1: It should throw exception when provider is not initialized.
    try {
      HadoopAccessorProvider.getFilesystem("", new org.apache.hadoop.fs.Path(""), new Configuration(false));
      fail();
    } catch (Exception e) {
      // ignore
    }

    HadoopAccessorProvider.init();

    // case2: when needing secured filesystem. but user is null. It should throw exception
    try {
      HadoopAccessorProvider.getFilesystem(null, new org.apache.hadoop.fs.Path("/user"), getSecuredConfiguration());
      fail();
    } catch (Exception e) {
      // ignore
    }

    // case3: when needing secured filesystem. but the initialized provider dont support kerberos.
    try {
      HadoopAccessorProvider.getFilesystem("alex", new org.apache.hadoop.fs.Path("/user"), getSecuredConfiguration());
      fail();
    } catch (Exception e) {
      // ignore
    }

    HadoopAccessorProvider.cleanup();
  }

  /**
   * Start the miniKdc to test the proxy user login.
   */
  @Test
  public void testUGILogin() throws Exception {
    assertTrue(UserGroupInformation.isSecurityEnabled());

    String principal = "foo";
    File keytab = new File(workDir, "foo.keytab");
    kdc.createPrincipal(keytab, principal);

    SecurityInfo securityInfo = SecurityInfo.newBuilder()
        .keytabFilePath(keytab.getAbsolutePath())
        .principal(principal)
        .reloginIntervalSec(1000L * 1000L)
        .build();
    HadoopAccessorProvider.init(securityInfo);

    assertTrue(UserGroupInformation.isLoginKeytabBased());
    assertEquals("foo", UserGroupInformation.getCurrentUser().getShortUserName());

    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    UserGroupInformation proxyUserUgi = UserGroupInformation.createProxyUser("boo", ugi);
    assertEquals("boo", proxyUserUgi.getShortUserName());

    // relogin
    HadoopAccessorProvider.kerberosRelogin();
    assertTrue(UserGroupInformation.isLoginKeytabBased());
    assertEquals("foo", UserGroupInformation.getCurrentUser().getShortUserName());

    HadoopAccessorProvider.cleanup();
  }

  /**
   * Write file by proxy user.
   *
   * @throws Exception
   */
  @Test
  public void testWriteByProxyUser() throws Exception {
    SecurityInfo securityInfo = SecurityInfo.newBuilder()
        .keytabFilePath(hdfsKeytab)
        .principal(hdfsPrincipal)
        .reloginIntervalSec(1000L * 1000L)
        .build();
    HadoopAccessorProvider.init(securityInfo);

    FileSystem proxyFs = HadoopAccessorProvider.getFilesystem(
        "alex",
        new org.apache.hadoop.fs.Path("/alex"),
        kerberizedDfsCluster.getFileSystem().getConf()
    );

    String fileContent = "hello world";
    FSDataOutputStream fsDataOutputStream =
        proxyFs.create(new org.apache.hadoop.fs.Path("/alex/proxy_user_written.txt"));
    BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, "UTF-8"));
    br.write(fileContent);
    br.close();

    FileStatus fileStatus =
        kerberizedDfsCluster.getFileSystem()
            .getFileStatus(new org.apache.hadoop.fs.Path("/alex/proxy_user_written.txt"));
    assertEquals("alex", fileStatus.getOwner());
  }

  /**
   * Test writing by super user to proxy real-user and then reading by real user from client.
   *
   * @throws Exception
   */
  @Test
  public void testWriteAndRead() throws Exception {
    FileSystem fileSystem = kerberizedDfsCluster.getFileSystem();
    String scheme = fileSystem.getScheme();
    assertEquals("hdfs", scheme);
    assertTrue(UserGroupInformation.isSecurityEnabled());

    SecurityInfo securityInfo = SecurityInfo.newBuilder()
        .keytabFilePath(hdfsKeytab)
        .principal(hdfsPrincipal)
        .reloginIntervalSec(1000L * 1000L)
        .build();
    HadoopAccessorProvider.init(securityInfo);

    // Write contents to file
    String fileContent = "hello world";
    FileSystem writeFs = HadoopAccessorProvider
        .getFileSystem(
            new org.apache.hadoop.fs.Path("/"),
            fileSystem.getConf()
        );
    boolean ok = writeFs.exists(new org.apache.hadoop.fs.Path("/alex"));
    assertTrue(ok);
    assertEquals("alex", writeFs.getFileStatus(new org.apache.hadoop.fs.Path("/alex")).getOwner());

    FileSystem proxyFs = HadoopAccessorProvider.getFilesystem(
        "alex",
        new org.apache.hadoop.fs.Path("/alex"),
        fileSystem.getConf()
    );
    FSDataOutputStream fsDataOutputStream =
        proxyFs.create(new org.apache.hadoop.fs.Path("/alex/test.txt"));
    BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, "UTF-8"));
    br.write(fileContent);
    br.close();

    boolean exists = proxyFs.exists(new org.apache.hadoop.fs.Path("/alex/test.txt"));
    assertTrue(exists);
    FileStatus fileStatus = proxyFs.getFileStatus(new org.apache.hadoop.fs.Path("/alex/test.txt"));
    assertEquals("alex", fileStatus.getOwner());

    // Read content from HDFS
    UserGroupInformation readerUGI = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        alexPrincipal + "@" + kdc.getRealm(),
        alexKeytab
    );
    readerUGI.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        FileSystem fs = FileSystem.get(fileSystem.getConf());
        FSDataInputStream inputStream = fs.open(new org.apache.hadoop.fs.Path("/alex/test.txt"));
        String fetchedResult = IOUtils.toString(inputStream);
        assertEquals(fileContent, fetchedResult);
        return null;
      }
    });

    HadoopAccessorProvider.cleanup();
  }

  private static Configuration getSecuredConfiguration() {
    Configuration conf = new Configuration(false);
    conf.set("hadoop.security.authentication", UserGroupInformation.AuthenticationMethod.KERBEROS.name());
    return conf;
  }
}
