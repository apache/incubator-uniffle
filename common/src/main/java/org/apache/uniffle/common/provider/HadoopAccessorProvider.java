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

import static org.apache.uniffle.common.config.RssBaseConf.RSS_ACCESS_HADOOP_KERBEROS_ENABLE;
import static org.apache.uniffle.common.config.RssBaseConf.RSS_ACCESS_HADOOP_KERBEROS_KEYTAB_FILE;
import static org.apache.uniffle.common.config.RssBaseConf.RSS_ACCESS_HADOOP_KERBEROS_PRINCIPAL;

import java.io.Closeable;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.uniffle.common.config.RssBaseConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

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
  private static final long RELOGIN_CHECK_INTERVAL_SEC = 60L;

  private static volatile HadoopAccessorProvider provider;

  private Map<String, UserGroupInformation> cache = new ConcurrentHashMap<>();
  private ScheduledExecutorService scheduledExecutorService;
  private boolean kerberosEnabled = false;
  private RssBaseConf rssBaseConf;

  private HadoopAccessorProvider(RssBaseConf rssConf) throws Exception {
    this.rssBaseConf = rssConf;
    this.kerberosEnabled = rssConf.getBoolean(RSS_ACCESS_HADOOP_KERBEROS_ENABLE);

    if (kerberosEnabled) {
      String keytabFile = rssConf.getString(RSS_ACCESS_HADOOP_KERBEROS_KEYTAB_FILE);
      if (StringUtils.isEmpty(keytabFile)) {
        throw new Exception("When hadoop kerberos is enabled. the conf of "
            + RSS_ACCESS_HADOOP_KERBEROS_KEYTAB_FILE.key() + " must be set");
      }

      String principal = rssConf.getString(RSS_ACCESS_HADOOP_KERBEROS_PRINCIPAL);
      if (StringUtils.isEmpty(principal)) {
        throw new Exception("When hadoop kerberos is enabled. the conf of "
            + RSS_ACCESS_HADOOP_KERBEROS_PRINCIPAL.key() + " must be set");
      }

      Configuration conf = new Configuration(false);
      conf.set("hadoop.security.authentication", "kerberos");

      UserGroupInformation.setConfiguration(conf);
      UserGroupInformation.loginUserFromKeytab(principal, keytabFile);

      cache = new ConcurrentHashMap<>();

      LOGGER.info("Got Kerberos ticket, keytab [{}], principal [{}], user [{}]",
          keytabFile, principal, UserGroupInformation.getLoginUser());

      scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder()
              .setDaemon(true).setNameFormat("Kerberos-Relogin-%d").build());
      scheduledExecutorService.scheduleAtFixedRate(
          HadoopAccessorProvider::kerberosRelogin,
          RELOGIN_CHECK_INTERVAL_SEC,
          RELOGIN_CHECK_INTERVAL_SEC,
          TimeUnit.SECONDS);
    }
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
    provider.cache.putIfAbsent(
        user,
        UserGroupInformation.createProxyUser(
            user, UserGroupInformation.getLoginUser()
        )
    );
    return provider.cache.get(user);
  }

  /**
   * The method is to return the Hadoop Filesystem directly which is not retrieved by
   * ugi proxy user.
   *
   * @param path
   * @param configuration
   * @return
   * @throws Exception
   */
  public static FileSystem getFileSystem(
      final Path path,
      final Configuration configuration) throws Exception {
    return getFilesystem(null, false, path, configuration);
  }

  /**
   * The only entrypoint is to get the hadoop filesystem instance and is compatible with
   * the kerberos security.
   *
   * @param user
   * @param path
   * @param conf
   * @return
   * @throws Exception
   */
  public static FileSystem getFilesystem(
      final String user,
      final boolean needSecuredFilesystem,
      final Path path,
      final Configuration conf) throws Exception {
    if (provider == null) {
      throw new Exception("HadoopAccessorProvider should be initialized");
    }
    if (needSecuredFilesystem && StringUtils.isEmpty(user)) {
      throw new Exception("User must be set when security is enabled");
    }
    if (needSecuredFilesystem && !provider.kerberosEnabled) {
      String msg = String.format("There is need to be interactive with secured DFS by user: %s, path: %s "
          + "but the HadoopAccessProvider's kerberos config is disabled and can't retrieve the "
          + "secured filesyste", user, path);
      throw new Exception(msg);
    }

    // For local file systems, return the raw local file system, such calls to flush()
    // actually flushes the stream.
    try {
      FileSystem fs;
      if (needSecuredFilesystem) {
        LOGGER.info("Fetched the proxy user ugi of {}", user);
        UserGroupInformation proxyUserUGI = provider.getProxyUser(user);
        fs = proxyUserUGI.doAs(new PrivilegedExceptionAction<FileSystem>() {
          @Override
          public FileSystem run() throws Exception {
            return path.getFileSystem(conf);
          }
        });
      } else {
        fs = path.getFileSystem(conf);
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

  public static void init(RssBaseConf rssBaseConf) throws Exception {
    if (provider == null) {
      synchronized (HadoopAccessorProvider.class) {
        if (provider == null) {
          final HadoopAccessorProvider hadoopAccessorProvider = new HadoopAccessorProvider(rssBaseConf);
          provider = hadoopAccessorProvider;
        }
      }
    }
    LOGGER.info("The {} has been initialized, kerberos enable: {}",
        HadoopAccessorProvider.class.getSimpleName(),
        provider.kerberosEnabled);
  }

  @VisibleForTesting
  static void cleanup() throws Exception {
    if (provider != null) {
      provider.close();
      provider = null;
    }
  }

  @Override
  public void close() throws IOException {
    if (cache != null) {
      for (UserGroupInformation ugi : cache.values()) {
        FileSystem.closeAllForUGI(ugi);
      }
      cache.clear();
    }
    if (scheduledExecutorService != null) {
      scheduledExecutorService.shutdown();
    }
  }
}
