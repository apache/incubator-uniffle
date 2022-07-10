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

import java.io.Closeable;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.util.ThreadUtils;

/**
 * The HadoopAccessorProvider will provide the only entrypoint to get the hadoop filesystem whether
 * the hadoop cluster is kerberized or not.
 * <p>
 * It should be initialized when the shuffle server/coordinator starts. And in client, there is no need
 * to login with keytab at startup, the authentication of client side should be managed by computing framework
 * like Spark/MR.
 */
public class HadoopAccessorProvider implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(HadoopAccessorProvider.class);

  private static volatile HadoopAccessorProvider provider;

  private final boolean kerberosEnabled;
  private ScheduledExecutorService scheduledExecutorService;

  private HadoopAccessorProvider(SecurityInfo securityInfo) throws Exception {
    if (securityInfo == null) {
      this.kerberosEnabled = false;
      return;
    }

    this.kerberosEnabled = true;

    String keytabFile = securityInfo.getKeytabFilePath();
    String principal = securityInfo.getPrincipal();
    long reLoginIntervalSec = securityInfo.getReloginIntervalSec();

    if (StringUtils.isEmpty(keytabFile)) {
      throw new Exception("When hadoop kerberos is enabled, keytab must be set");
    }

    if (StringUtils.isEmpty(principal)) {
      throw new Exception("When hadoop kerberos is enabled, principal must be set");
    }

    if (reLoginIntervalSec <= 0) {
      throw new Exception("The relogin interval must be negative");
    }

    Configuration conf = new Configuration(false);
    conf.set("hadoop.security.authentication", "kerberos");

    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.loginUserFromKeytab(principal, keytabFile);

    LOGGER.info("Got Kerberos ticket, keytab [{}], principal [{}], user [{}]",
        keytabFile, principal, UserGroupInformation.getLoginUser());

    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        ThreadUtils.getThreadFactory("Kerberos-relogin-%d")
    );
    scheduledExecutorService.scheduleAtFixedRate(
        HadoopAccessorProvider::kerberosRelogin,
        reLoginIntervalSec,
        reLoginIntervalSec,
        TimeUnit.SECONDS);
  }

  @VisibleForTesting
  static void kerberosRelogin() {
    try {
      LOGGER.info("Renewing kerberos token.");
      UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
    } catch (Throwable t) {
      LOGGER.error("Error in token renewal task: ", t);
    }
  }

  private static UserGroupInformation getProxyUser(String user) throws IOException {
    return UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
  }

  private static FileSystem getInternalFileSystem(
      final String user,
      final boolean retrievedByProxyUser,
      final Path path,
      final Configuration configuration) throws Exception {
    if (provider == null) {
      throw new Exception("HadoopAccessorProvider should be initialized.");
    }

    if (retrievedByProxyUser && StringUtils.isEmpty(user)) {
      throw new Exception("User must be set when security is enabled");
    }
    if (retrievedByProxyUser && !provider.kerberosEnabled) {
      String msg = String.format("There is need to be interactive with secured DFS by user: %s, path: %s "
          + "but the HadoopAccessProvider's kerberos config is disabled and can't retrieve the "
          + "secured filesystem", user, path);
      throw new Exception(msg);
    }

    // For local file systems, return the raw local file system, such calls to flush()
    // actually flushes the stream.
    try {
      FileSystem fs;
      if (retrievedByProxyUser) {
        LOGGER.info("Fetching the proxy user ugi of {} when getting filesystem of [{}]", user, path);
        UserGroupInformation proxyUserUGI = provider.getProxyUser(user);
        fs = proxyUserUGI.doAs(new PrivilegedExceptionAction<FileSystem>() {
          @Override
          public FileSystem run() throws Exception {
            return path.getFileSystem(configuration);
          }
        });
      } else {
        fs = path.getFileSystem(configuration);
      }
      if (fs instanceof LocalFileSystem) {
        LOGGER.debug("{} is local file system", path);
        return ((LocalFileSystem) fs).getRawFileSystem();
      }
      return fs;
    } catch (IOException e) {
      LOGGER.error("Fail to get filesystem of {}", path);
      throw e;
    }
  }

  /**
   * The only entrypoint is to get the hadoop filesystem instance and is compatible with
   * the kerberos security.
   *
   * When to invoke this method?
   * 1. For shuffle server side, it needs to get filesystem before writing the shuffle data to secured HDFS
   * with the spark job's user auth.
   *
   * @param user
   * @param path
   * @param conf
   * @return
   * @throws Exception
   */
  public static FileSystem getFilesystem(
      final String user,
      final Path path,
      final Configuration conf) throws Exception {
    UserGroupInformation.AuthenticationMethod authenticationMethod =
        SecurityUtil.getAuthenticationMethod(conf);
    boolean securityEnable = authenticationMethod != UserGroupInformation.AuthenticationMethod.SIMPLE;
    return getInternalFileSystem(user, securityEnable, path, conf);
  }

  /**
   * The method is to return the Hadoop Filesystem directly which is not retrieved by
   * ugi proxy user.
   *
   * When to invoke this method?
   * 1. In client side, spark shuffle-reader getting filesystem before reading shuffle data stored in HDFS.
   * 2. In shuffle-server/coordinator side, it reads the config file stored in HDFS.
   *
   * @param path
   * @param configuration
   * @return
   * @throws Exception
   */
  public static FileSystem getFileSystem(
      final Path path,
      final Configuration configuration) throws Exception {
    return getInternalFileSystem(null, false, path, configuration);
  }

  /**
   * For kerberized cluster access
   * @param securityInfo
   * @throws Exception
   */
  public static void init(SecurityInfo securityInfo) throws Exception {
    if (provider == null) {
      synchronized (HadoopAccessorProvider.class) {
        if (provider == null) {
          final HadoopAccessorProvider hadoopAccessorProvider = new HadoopAccessorProvider(securityInfo);
          provider = hadoopAccessorProvider;
        }
      }
    }
    LOGGER.info("The {} has been initialized, kerberos enable: {}",
        HadoopAccessorProvider.class.getSimpleName(),
        provider.kerberosEnabled);
  }

  /**
   * No need to
   * For non-kerberized cluster access, like client side reading shuffle data.
   */
  public static void init() throws Exception {
    init(null);
  }

  @VisibleForTesting
  public static void cleanup() throws Exception {
    LOGGER.info("Clear the Hadoop provider.");
    if (provider != null) {
      provider.close();
      provider = null;
    }
  }

  @Override
  public void close() throws IOException {
    if (scheduledExecutorService != null) {
      scheduledExecutorService.shutdown();
    }
  }
}
