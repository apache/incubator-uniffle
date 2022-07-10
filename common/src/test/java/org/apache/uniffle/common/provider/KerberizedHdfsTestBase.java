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
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ImpersonationProvider;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HTTP_POLICY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KerberizedHdfsTestBase implements Serializable {
  private static final Logger LOGGER = LoggerFactory.getLogger(KerberizedHdfsTestBase.class);

  protected static MiniKdc kdc;
  protected static File workDir;
  protected static Path tempDir;
  protected static Path kerberizedDfsBaseDir;

  protected static MiniDFSCluster kerberizedDfsCluster;

  private static Class testRunnerCls = KerberizedHdfsTestBase.class;

  // The super user for accessing HDFS
  protected static String hdfsKeytab;
  protected static String hdfsPrincipal;
  // The normal user of alex for accessing HDFS
  protected static String alexKeytab;
  protected static String alexPrincipal;

  public static void setup() throws Exception {
    tempDir = Files.createTempDirectory("tempDir").toFile().toPath();
    kerberizedDfsBaseDir = Files.createTempDirectory("kerberizedDfsBaseDir").toFile().toPath();

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
    FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL, false);
    writeFs.setPermission(new org.apache.hadoop.fs.Path("/alex"), permission);

    writeFs.setPermission(new org.apache.hadoop.fs.Path("/"), permission);

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

    conf.set(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_IMPERSONATION_PROVIDER_CLASS,
        "org.apache.uniffle.common.provider.KerberizedHdfsTestBase$TestDummyImpersonationProvider");

    String keystoresDir = kerberizedDfsBaseDir.toFile().getAbsolutePath();
    String sslConfDir = KeyStoreTestUtil.getClasspathDir(testRunnerCls);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);

    return conf;
  }

  private static void startKerberizedDFS() throws Exception {
    String principal = "hdfs" + "/localhost";
    File keytab = new File(workDir, "hdfs.keytab");
    kdc.createPrincipal(keytab, principal);
    hdfsKeytab = keytab.getPath();
    hdfsPrincipal = principal + "@" + kdc.getRealm();

    Configuration conf = new Configuration();
    conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    String krb5Conf = kdc.getKrb5conf().getAbsolutePath();
    System.setProperty("java.security.krb5.conf", krb5Conf);
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.setShouldRenewImmediatelyForTests(true);
    UserGroupInformation ugi =
        UserGroupInformation.loginUserFromKeytabAndReturnUGI(hdfsPrincipal, hdfsKeytab);

    Configuration hdfsConf = createSecureDFSConfig();
    hdfsConf.set("hadoop.proxyuser.hdfs.hosts", "*");
    hdfsConf.set("hadoop.proxyuser.hdfs.groups", "*");
    hdfsConf.set("hadoop.proxyuser.hdfs.users", "*");

    kerberizedDfsCluster = ugi.doAs(new PrivilegedExceptionAction<MiniDFSCluster>() {
      @Override
      public MiniDFSCluster run() throws Exception {
        return new MiniDFSCluster
            .Builder(hdfsConf)
            .numDataNodes(1)
            .clusterId("kerberized-cluster-1")
            .checkDataNodeAddrConfig(true)
            .build();
      }
    });
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
  }

  public static void tearDown() throws IOException {
    if (kerberizedDfsCluster != null) {
      kerberizedDfsCluster.close();
    }
    if (kdc != null) {
      kdc.stop();
    }
    setTestRunner(KerberizedHdfsTestBase.class);
    UserGroupInformation.reset();
  }

  public static String getSchemeAndAuthorityPrefix() {
    return String.format("hdfs://localhost:%s/", kerberizedDfsCluster.getNameNodePort());
  }

  public static Configuration getConf() throws IOException {
    Configuration configuration = kerberizedDfsCluster.getFileSystem().getConf();
    configuration.setBoolean("fs.hdfs.impl.disable.cache", true);
    configuration.set("hadoop.security.authentication", UserGroupInformation.AuthenticationMethod.KERBEROS.name());
    return configuration;
  }

  public static FileSystem getFileSystem() throws Exception {
    return kerberizedDfsCluster.getFileSystem();
  }

  /**
   * Should be invoked by extending class to solve the NPE.
   * refer to: https://github.com/apache/hbase/pull/1207
   *
   * @param testRunnerCls
   */
  public static void setTestRunner(Class testRunnerCls) {
    KerberizedHdfsTestBase.testRunnerCls = testRunnerCls;
  }

  static class TestDummyImpersonationProvider implements ImpersonationProvider {

    @Override
    public void init(String configurationPrefix) {
      // ignore
    }

    /**
     * Allow the user of HDFS can be delegated to alex.
     */
    @Override
    public void authorize(UserGroupInformation userGroupInformation, String s) throws AuthorizationException {
      UserGroupInformation superUser = userGroupInformation.getRealUser();
      LOGGER.info("Proxy: {}", superUser);
    }

    @Override
    public void setConf(Configuration conf) {
      // ignore
    }

    @Override
    public Configuration getConf() {
      return null;
    }
  }
}
