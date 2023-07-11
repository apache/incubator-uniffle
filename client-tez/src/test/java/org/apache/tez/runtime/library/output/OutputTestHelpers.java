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

package org.apache.tez.runtime.library.output;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.runtime.api.MemoryUpdateCallback;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.OutputStatisticsReporter;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class OutputTestHelpers {

  public static final ApplicationId APP_ID = ApplicationId.newInstance(1681717153064L, 3601637);
  public static final ApplicationAttemptId APP_ATTEMPT_ID = ApplicationAttemptId.newInstance(APP_ID, 1);

  /**
   * help to create output context
   */
  public static OutputContext createOutputContext(Configuration conf, Path workingDir) throws IOException {
    OutputContext ctx = mock(OutputContext.class);

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        long requestedSize = (Long) invocation.getArguments()[0];
        MemoryUpdateCallbackHandler callback = (MemoryUpdateCallbackHandler) invocation
            .getArguments()[1];
        callback.memoryAssigned(requestedSize);
        return null;
      }
    }).when(ctx).requestInitialMemory(anyLong(), any(MemoryUpdateCallback.class));
    doReturn(TezUtils.createUserPayloadFromConf(conf)).when(ctx).getUserPayload();
    doReturn("Map 1").when(ctx).getTaskVertexName();
    doReturn("Reducer 2").when(ctx).getDestinationVertexName();
    doReturn("attempt_1681717153064_3601637_1_13_000096_0").when(ctx).getUniqueIdentifier();
    doReturn(new String[] { workingDir.toString() }).when(ctx).getWorkDirs();
    doReturn(200 * 1024 * 1024L).when(ctx).getTotalMemoryAvailableToTask();
    doReturn(new TezCounters()).when(ctx).getCounters();
    OutputStatisticsReporter statsReporter = mock(OutputStatisticsReporter.class);
    doReturn(statsReporter).when(ctx).getStatisticsReporter();
    doReturn(new ExecutionContextImpl("localhost")).when(ctx).getExecutionContext();
    doReturn(APP_ID).when(ctx).getApplicationId();
    return ctx;
  }
}
