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

package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezExecutors;
import org.apache.tez.common.TezSharedExecutor;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.apache.tez.runtime.library.common.shuffle.impl.RssShuffleManagerTest.APPATTEMPT_ID;
import static org.apache.tez.runtime.library.common.shuffle.impl.RssShuffleManagerTest.APP_ID;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RssShuffleTest {

  private TezExecutors sharedExecutor;

  @BeforeEach
  public void setup() {
    sharedExecutor = new TezSharedExecutor(new Configuration());
  }

  @BeforeEach
  public void cleanup() {
    sharedExecutor.shutdownNow();
  }

  @Test
  @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
  public void testSchedulerTerminatesOnException() throws IOException, InterruptedException {
    try (MockedStatic<ShuffleUtils> shuffleUtils = Mockito.mockStatic(ShuffleUtils.class)) {
      shuffleUtils.when(() -> ShuffleUtils.deserializeShuffleProviderMetaData(any())).thenReturn(4);

      InputContext inputContext = createTezInputContext();
      TezConfiguration conf = new TezConfiguration();
      conf.setLong(Constants.TEZ_RUNTIME_TASK_MEMORY, 300000L);
      RssShuffle shuffle = new RssShuffle(inputContext, conf, 1, 3000000L, 0, APPATTEMPT_ID);
      try {
        ShuffleScheduler scheduler = shuffle.rssScheduler;
        MergeManager mergeManager = shuffle.merger;
        assertFalse(scheduler.isShutdown());
        assertFalse(mergeManager.isShutdown());
        shuffle.run();

        String exceptionMessage = "Simulating fetch failure";
        shuffle.reportException(new IOException(exceptionMessage));

        while (!scheduler.isShutdown()) {
          Thread.sleep(100L);
        }
        assertTrue(scheduler.isShutdown());

        while (!mergeManager.isShutdown()) {
          Thread.sleep(100L);
        }
        assertTrue(mergeManager.isShutdown());
      } finally {
        shuffle.shutdown();
      }
    }
  }

  @Test
  @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
  public void testKillSelf() throws IOException, InterruptedException {
    try (MockedStatic<ShuffleUtils> shuffleUtils = Mockito.mockStatic(ShuffleUtils.class)) {
      shuffleUtils.when(() -> ShuffleUtils.deserializeShuffleProviderMetaData(any())).thenReturn(4);

      InputContext inputContext = createTezInputContext();
      TezConfiguration conf = new TezConfiguration();
      conf.setLong(Constants.TEZ_RUNTIME_TASK_MEMORY, 300000L);
      RssShuffle shuffle = new RssShuffle(inputContext, conf, 1, 3000000L, 0, APPATTEMPT_ID);
      try {
        ShuffleScheduler scheduler = shuffle.rssScheduler;
        assertFalse(scheduler.isShutdown());
        shuffle.run();

        // killSelf() would invoke close(). Internally Shuffle --> merge.close() --> finalMerge()
        // gets called. In MergeManager::finalMerge(), it would throw illegal argument exception
        // as
        // uniqueIdentifier is not present in inputContext. This is used as means of simulating
        // exception.
        scheduler.killSelf(new Exception(), "due to internal error");
        assertTrue(scheduler.isShutdown());

        // killSelf() should not result in reporting failure to AM
        ArgumentCaptor<Throwable> throwableArgumentCaptor =
            ArgumentCaptor.forClass(Throwable.class);
        ArgumentCaptor<String> stringArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(inputContext, times(0))
            .reportFailure(
                eq(TaskFailureType.NON_FATAL),
                throwableArgumentCaptor.capture(),
                stringArgumentCaptor.capture());
      } finally {
        shuffle.shutdown();
      }
    }
  }

  private InputContext createTezInputContext() throws IOException {
    ApplicationId applicationId = ApplicationId.newInstance(1, 1);
    InputContext inputContext = mock(InputContext.class);
    doReturn(applicationId).when(inputContext).getApplicationId();
    doReturn("Map 1").when(inputContext).getSourceVertexName();
    doReturn("Reducer 1").when(inputContext).getTaskVertexName();
    String uniqueId =
        String.format(
            "%s_%05d",
            TezTaskAttemptID.getInstance(
                TezTaskID.getInstance(
                    TezVertexID.getInstance(TezDAGID.getInstance(applicationId, 1), 1), 1),
                1),
            1);
    doReturn(uniqueId).when(inputContext).getUniqueIdentifier();
    when(inputContext.getCounters()).thenReturn(new TezCounters());
    ExecutionContext executionContext = new ExecutionContextImpl("localhost");
    doReturn(executionContext).when(inputContext).getExecutionContext();
    ByteBuffer shuffleBuffer = ByteBuffer.allocate(4).putInt(0, 4);
    doReturn(shuffleBuffer).when(inputContext).getServiceProviderMetaData(anyString());
    Token<JobTokenIdentifier> sessionToken =
        new Token<JobTokenIdentifier>(
            new JobTokenIdentifier(new Text("text")), new JobTokenSecretManager());
    ByteBuffer tokenBuffer = TezCommonUtils.serializeServiceData(sessionToken);
    doReturn(tokenBuffer).when(inputContext).getServiceConsumerMetaData(anyString());
    doReturn(APP_ID).when(inputContext).getApplicationId();
    doReturn(APPATTEMPT_ID.getAttemptId()).when(inputContext).getDAGAttemptNumber();
    return inputContext;
  }
}
