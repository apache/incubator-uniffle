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

package org.apache.uniffle.storage.handler.impl;

import java.io.IOException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.provider.HadoopAccessorProvider;
import org.apache.uniffle.common.provider.KerberizedHdfsTestBase;
import org.apache.uniffle.common.provider.SecurityInfo;

public class KerberizedHdfsClientReadHandlerTest extends KerberizedHdfsTestBase {
  static {
    KerberizedHdfsTestBase.setTestRunner(KerberizedHdfsClientReadHandlerTest.class);
  }

  @BeforeAll
  public static void beforeAll() throws Exception {
    KerberizedHdfsTestBase.setup();
    HadoopAccessorProvider.init(
        SecurityInfo
            .newBuilder()
            .keytabFilePath(hdfsKeytab)
            .principal(hdfsPrincipal)
            .reloginIntervalSec(1000L * 1000L)
            .build()
    );
  }

  @AfterAll
  public static void afterAll() throws Exception {
    HadoopAccessorProvider.cleanup();
    KerberizedHdfsTestBase.tearDown();
  }

  @Test
  public void test() throws IOException {
    HdfsClientReadHandlerTest.createAndRunCases(
        getSchemeAndAuthorityPrefix(),
        getConf(),
        "alex"
    );
  }
}
