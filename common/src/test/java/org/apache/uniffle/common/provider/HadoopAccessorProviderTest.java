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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HTTP_POLICY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
import static org.apache.uniffle.common.config.RssBaseConf.RSS_ACCESS_HADOOP_KERBEROS_ENABLE;
import static org.apache.uniffle.common.config.RssBaseConf.RSS_ACCESS_HADOOP_KERBEROS_KEYTAB_FILE;
import static org.apache.uniffle.common.config.RssBaseConf.RSS_ACCESS_HADOOP_KERBEROS_PRINCIPAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.OutputStreamWriter;
import java.nio.file.Path;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.uniffle.common.config.RssBaseConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class HadoopAccessorProviderTest {
  private static MiniKdc kdc;
  private static File workDir;
  @TempDir
  private static Path tempDir;
  @TempDir
  private static Path kerberizedDfsBaseDir;

  private static MiniDFSCluster kerberizedDfsCluster;
  private static MiniDFSCluster nonKerberizedDfsCluster;

  // The super user for accessing HDFS
  private static String hdfsKeytab;
  private static String hdfsPrincipal;
  // The normal user of alex for accessing HDFS
  private static String alexKeytab;
  private static String alexPrincipal;

  @BeforeAll
  public static void setup() throws Exception {
    startKDC();
    startKerberizedDFS();
    setupDFSData();
  }

  private static void setupDFSData() throws Exception {
    String principal = "alex/localhost";
    File keytab = new File(workDir, "alex.keytab");
    kdc.createPrincipal(keytab, principal);
    alexKeytab = keytab.getAbsolutePath();
    alexPrincipal = principal;

    FileSystem writeFs = kerberizedDfsCluster.getFileSystem();
    boolean ok = writeFs.exists(new org.apache.hadoop.fs.Path("/alex"));
    assertFalse(ok);
    ok = writeFs.mkdirs(new org.apache.hadoop.fs.Path("/alex"));
    assertTrue(ok);

    writeFs.setOwner(new org.apache.hadoop.fs.Path("/alex"), "alex", "alex");
    FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE, false);
    writeFs.setPermission(new org.apache.hadoop.fs.Path("/alex"), permission);

    String oneFileContent = "test content";
    FSDataOutputStream fsDataOutputStream =
        writeFs.create(new org.apache.hadoop.fs.Path("/alex/basic.txt"));
    BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, "UTF-8"));
    br.write(oneFileContent);
    br.close();

    writeFs.setOwner(new org.apache.hadoop.fs.Path("/alex/basic.txt"), "alex", "alex");
    writeFs.setPermission(new org.apache.hadoop.fs.Path("/alex/basic.txt"), permission);
  }

  private static Configuration createSecureDFSConfig() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, conf);

    conf.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    conf.set(DFS_NAMENODE_KEYTAB_FILE_KEY, hdfsKeytab);
    conf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    conf.set(DFS_DATANODE_KEYTAB_FILE_KEY, hdfsKeytab);
    conf.set(DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    conf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication");
    conf.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    conf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY, 10);

    // https://issues.apache.org/jira/browse/HDFS-7431
    conf.set(DFS_ENCRYPT_DATA_TRANSFER_KEY, "true");

    // SSL conf.
    String keystoresDir = kerberizedDfsBaseDir.toFile().getAbsolutePath();
    String sslConfDir = KeyStoreTestUtil.getClasspathDir(HadoopAccessorProvider.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);

    return conf;
  }

  private static void startKerberizedDFS() throws Exception {
    String principal = "hdfs" + "/localhost";
    File keytab = new File(workDir, "hdfs.keytab");
    kdc.createPrincipal(keytab, principal);
    hdfsKeytab = keytab.getPath();
    hdfsPrincipal = principal + "@" + kdc.getRealm();

    UserGroupInformation.loginUserFromKeytab(hdfsPrincipal, hdfsKeytab);
    Configuration hdfsConf = createSecureDFSConfig();
    hdfsConf.set("hadoop.proxyuser.hdfs.groups", "*");
    hdfsConf.set("hadoop.proxyuser.hdfs.hosts", "*");

    kerberizedDfsCluster = new MiniDFSCluster
        .Builder(hdfsConf)
        .numDataNodes(1)
        .checkDataNodeAddrConfig(true)
        .build();
  }

  private static void startKDC() throws Exception {
    Properties kdcConf = MiniKdc.createConf();
    String hostName = "localhost";
    kdcConf.setProperty(MiniKdc.INSTANCE, HadoopAccessorProviderTest.class.getSimpleName());
    kdcConf.setProperty(MiniKdc.ORG_NAME, HadoopAccessorProviderTest.class.getSimpleName());
    kdcConf.setProperty(MiniKdc.ORG_DOMAIN, "COM");
    kdcConf.setProperty(MiniKdc.KDC_BIND_ADDRESS, hostName);
    kdcConf.setProperty(MiniKdc.KDC_PORT, "0");
    workDir = tempDir.toFile();
    kdc = new MiniKdc(kdcConf, workDir);
    kdc.start();

    Configuration conf = new Configuration();
    conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    String krb5Conf = kdc.getKrb5conf().getAbsolutePath();
    System.setProperty("java.security.krb5.conf", krb5Conf);
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.setShouldRenewImmediatelyForTests(true);
  }

  @AfterAll
  public static void tearDown() {
    if (kdc != null) {
      kdc.stop();
    }
    if (kerberizedDfsCluster != null) {
      kerberizedDfsCluster.close();
    }
  }

  @Test
  public void testIllegalInitialization() throws Exception {
    RssBaseConf rssBaseConf = new RssBaseConf();
    rssBaseConf.setBoolean(RSS_ACCESS_HADOOP_KERBEROS_ENABLE, true);
    try {
      HadoopAccessorProvider.init(rssBaseConf);
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
      HadoopAccessorProvider.getFilesystem("", false, new org.apache.hadoop.fs.Path(""), new Configuration(false));
      fail();
    } catch (Exception e) {
      // ignore
    }

    RssBaseConf rssBaseConf = new RssBaseConf();
    rssBaseConf.setBoolean(RSS_ACCESS_HADOOP_KERBEROS_ENABLE, false);
    HadoopAccessorProvider.init(rssBaseConf);

    // case2: when needing secured filesystem. but user is null. It should throw exception
    try {
      HadoopAccessorProvider.getFilesystem(null, true, new org.apache.hadoop.fs.Path("/user"), new Configuration());
      fail();
    } catch (Exception e) {
      // ignore
    }

    // case3: when needing secured filesystem. but the initialized provider dont support kerberos.
    try {
      HadoopAccessorProvider.getFilesystem("alex", true, new org.apache.hadoop.fs.Path("/user"), new Configuration());
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

    RssBaseConf conf = new RssBaseConf();
    conf.setBoolean(RSS_ACCESS_HADOOP_KERBEROS_ENABLE, true);
    conf.setString(RSS_ACCESS_HADOOP_KERBEROS_KEYTAB_FILE, keytab.getAbsolutePath());
    conf.setString(RSS_ACCESS_HADOOP_KERBEROS_PRINCIPAL, principal);

    HadoopAccessorProvider.init(conf);
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
    RssBaseConf conf = new RssBaseConf();
    conf.setBoolean(RSS_ACCESS_HADOOP_KERBEROS_ENABLE, true);
    conf.setString(RSS_ACCESS_HADOOP_KERBEROS_KEYTAB_FILE, hdfsKeytab);
    conf.setString(RSS_ACCESS_HADOOP_KERBEROS_PRINCIPAL, hdfsPrincipal);
    HadoopAccessorProvider.init(conf);

    FileSystem proxyFs = HadoopAccessorProvider.getFilesystem(
        "alex",
        true,
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

    RssBaseConf conf = new RssBaseConf();
    conf.setBoolean(RSS_ACCESS_HADOOP_KERBEROS_ENABLE, true);
    conf.setString(RSS_ACCESS_HADOOP_KERBEROS_KEYTAB_FILE, hdfsKeytab);
    conf.setString(RSS_ACCESS_HADOOP_KERBEROS_PRINCIPAL, hdfsPrincipal);
    HadoopAccessorProvider.init(conf);

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
        true,
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
}
