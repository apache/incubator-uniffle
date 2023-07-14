/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime.library.input;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.IdUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.RssShuffle;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.Shuffle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.apache.tez.common.RssTezConfig.RSS_SHUFFLE_DESTINATION_VERTEX_ID;
import static org.apache.tez.common.RssTezConfig.RSS_SHUFFLE_SOURCE_VERTEX_ID;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RssOrderedGroupedKVInputTest {

  @Test
  @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
  public void testInterruptWhileAwaitingInput() throws IOException {
    try (MockedStatic<IdUtils> idUtils = Mockito.mockStatic(IdUtils.class)) {
      ApplicationId appId = ApplicationId.newInstance(9999, 72);
      ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 1);
      idUtils.when(IdUtils::getApplicationAttemptId).thenReturn(appAttemptId);
      try (MockedStatic<ShuffleUtils> shuffleUtils = Mockito.mockStatic(ShuffleUtils.class)) {
        shuffleUtils.when(() -> ShuffleUtils.deserializeShuffleProviderMetaData(any())).thenReturn(4);
        InputContext inputContext = createMockInputContext();
        RssOrderedGroupedKVInput kvInput = new OrderedGroupedKVInputForTest(inputContext, 10);
        kvInput.initialize();
        kvInput.start();
        try {
          kvInput.getReader();
          fail("getReader should not return since underlying inputs are not ready");
        } catch (Exception e) {
          assertTrue(e instanceof RssShuffle.RssShuffleError);
        }
      }
    }
  }

  private InputContext createMockInputContext() throws IOException {
    InputContext inputContext = mock(InputContext.class);
    doReturn("Map 1").when(inputContext).getSourceVertexName();
    doReturn("Reducer 1").when(inputContext).getTaskVertexName();
    when(inputContext.getUniqueIdentifier()).thenReturn("attempt_1685094627632_0157_1_01_000000_0_10006");

    ApplicationId applicationId = ApplicationId.newInstance(1, 1);
    doReturn(applicationId).when(inputContext).getApplicationId();

    ExecutionContext executionContext = new ExecutionContextImpl("localhost");
    doReturn(executionContext).when(inputContext).getExecutionContext();

    Configuration conf = new TezConfiguration();
    conf.setInt(RSS_SHUFFLE_SOURCE_VERTEX_ID, 1);
    conf.setInt(RSS_SHUFFLE_DESTINATION_VERTEX_ID, 2);
    UserPayload payLoad = TezUtils.createUserPayloadFromConf(conf);
    String[] workingDirs = new String[]{"workDir1"};
    TezCounters counters = new TezCounters();

    doReturn(payLoad).when(inputContext).getUserPayload();
    doReturn(workingDirs).when(inputContext).getWorkDirs();
    doReturn(200 * 1024 * 1024L).when(inputContext).getTotalMemoryAvailableToTask();
    doReturn(counters).when(inputContext).getCounters();

    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();

        if (args[1] instanceof MemoryUpdateCallbackHandler) {
          MemoryUpdateCallbackHandler memUpdateCallbackHandler = (MemoryUpdateCallbackHandler) args[1];
          memUpdateCallbackHandler.memoryAssigned(200 * 1024 * 1024);
        } else {
          fail();
        }
        return null;
      }
    }).when(inputContext).requestInitialMemory(any(long.class),
        any(MemoryUpdateCallbackHandler.class));
    return inputContext;
  }

  static class OrderedGroupedKVInputForTest extends RssOrderedGroupedKVInput {
    OrderedGroupedKVInputForTest(InputContext inputContext, int numPhysicalInputs) {
      super(inputContext, numPhysicalInputs);
    }

    Shuffle createShuffle() throws IOException {
      Shuffle shuffle = mock(Shuffle.class);
      try {
        doThrow(new InterruptedException()).when(shuffle).waitForInput();
      } catch (InterruptedException e) {
        fail();
      } catch (Exception e) {
        fail();
      }
      return shuffle;
    }
  }
}
