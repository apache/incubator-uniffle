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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.ThreadUtils;

public class HadoopSecurityContext implements SecurityContext {
  private static final Logger LOGGER = LoggerFactory.getLogger(HadoopSecurityContext.class);
  private static final String KRB5_CONF_KEY = "java.security.krb5.conf";

  private UserGroupInformation loginUgi;
  private ScheduledExecutorService refreshScheduledExecutor;

  // The purpose of the proxy user ugi cache is to prevent the creation of
  // multiple cache keys for the same user, scheme, and authority in the Hadoop filesystem.
  // Without this cache, large amounts of unnecessary filesystem instances could be stored in
  // memory,
  // leading to potential memory leaks. For more information on this issue, refer to #706.
  private Map<String, UserGroupInformation> proxyUserUgiPool;

  public HadoopSecurityContext(
      String krb5ConfPath, String keytabFile, String principal, long refreshIntervalSec)
      throws Exception {
    if (StringUtils.isEmpty(keytabFile)) {
      throw new IllegalArgumentException("KeytabFilePath must be not null or empty");
    }
    if (StringUtils.isEmpty(principal)) {
      throw new IllegalArgumentException("principal must be not null or empty");
    }
    if (refreshIntervalSec <= 0) {
      throw new IllegalArgumentException("refreshIntervalSec must be not negative");
    }

    if (StringUtils.isNotEmpty(krb5ConfPath)) {
      System.setProperty(KRB5_CONF_KEY, krb5ConfPath);
    }

    Configuration conf = new Configuration(false);
    conf.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(conf);

    this.loginUgi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabFile);

    LOGGER.info(
        "Got Kerberos ticket, keytab [{}], principal [{}], user [{}]",
        keytabFile,
        principal,
        loginUgi.getShortUserName());

    refreshScheduledExecutor =
        ThreadUtils.getDaemonSingleThreadScheduledExecutor("Kerberos-refresh");
    refreshScheduledExecutor.scheduleAtFixedRate(
        this::authRefresh, refreshIntervalSec, refreshIntervalSec, TimeUnit.SECONDS);
    proxyUserUgiPool = JavaUtils.newConcurrentMap();
  }

  private void authRefresh() {
    try {
      LOGGER.info("Renewing kerberos token.");
      loginUgi.checkTGTAndReloginFromKeytab();
    } catch (Throwable t) {
      LOGGER.error("Error in token renewal task: ", t);
    }
  }

  @Override
  public <T> T runSecured(String user, Callable<T> securedCallable) throws Exception {
    if (StringUtils.isEmpty(user)) {
      throw new Exception("User must be not null or empty");
    }

    // Run with the proxy user.
    if (!user.equals(loginUgi.getShortUserName())) {
      UserGroupInformation proxyUserUgi =
          proxyUserUgiPool.computeIfAbsent(
              user, x -> UserGroupInformation.createProxyUser(x, loginUgi));
      return executeWithUgiWrapper(proxyUserUgi, securedCallable);
    }

    // Run with the current login user.
    return executeWithUgiWrapper(loginUgi, securedCallable);
  }

  @Override
  public String getContextLoginUser() {
    return loginUgi.getShortUserName();
  }

  private <T> T executeWithUgiWrapper(UserGroupInformation ugi, Callable<T> callable)
      throws Exception {
    return ugi.doAs((PrivilegedExceptionAction<T>) callable::call);
  }

  // Only for tests
  @VisibleForTesting
  Map<String, UserGroupInformation> getProxyUserUgiPool() {
    return proxyUserUgiPool;
  }

  @Override
  public void close() throws IOException {
    if (refreshScheduledExecutor != null) {
      refreshScheduledExecutor.shutdown();
    }
    if (proxyUserUgiPool != null) {
      proxyUserUgiPool.clear();
      proxyUserUgiPool = null;
    }
  }
}
