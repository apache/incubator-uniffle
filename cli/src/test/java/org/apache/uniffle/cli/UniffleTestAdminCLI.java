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

package org.apache.uniffle.cli;

import java.io.IOException;
import java.util.HashMap;

import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.Mockito;

import org.apache.uniffle.UniffleCliArgsException;
import org.apache.uniffle.common.util.http.UniffleRestClient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class UniffleTestAdminCLI {

  @Test
  public void testAdminRefreshCLI() throws UniffleCliArgsException, IOException {

    UniffleRestClient client = Mockito.mock(UniffleRestClient.class, Answers.RETURNS_DEEP_STUBS);
    Mockito.when(
            client.getHttpClient().get(Mockito.anyString(), Mockito.anyMap(), Mockito.anyString()))
        .thenReturn("OK");
    UniffleAdminCLI uniffleAdminCLI = new UniffleAdminCLI("", "", client);
    String[] args = {"-r"};
    assertEquals(0, uniffleAdminCLI.run(args));
    Mockito.verify(client.getHttpClient(), Mockito.times(1))
        .get("/api/admin/refreshChecker", new HashMap<>(), null);
  }

  @Test
  public void testMissingClientCLI() throws UniffleCliArgsException, IOException {
    UniffleAdminCLI uniffleAdminCLI = new UniffleAdminCLI("", "");
    String[] args = {"-r"};
    assertThrows(UniffleCliArgsException.class, () -> uniffleAdminCLI.run(args));
  }
}
