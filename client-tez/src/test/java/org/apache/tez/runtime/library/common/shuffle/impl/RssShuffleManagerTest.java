/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime.library.common.shuffle.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.token.Token;
import org.apache.tez.common.TezExecutors;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezSharedExecutor;
import org.apache.tez.common.UmbilicalUtils;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.http.HttpConnectionParams;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.FetchResult;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput;
import org.apache.tez.runtime.library.common.shuffle.FetchedInputAllocator;
import org.apache.tez.runtime.library.common.shuffle.Fetcher;
import org.apache.tez.runtime.library.common.shuffle.InputHost;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.uniffle.common.ShuffleServerInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RssShuffleManagerTest {
  private static final String FETCHER_HOST = "localhost";
  private static final int PORT = 8080;
  private static final String PATH_COMPONENT = "attempttmp";
  private static final Configuration conf = new Configuration();
  private static TezExecutors sharedExecutor;

  @BeforeAll
  public static void setup() {
    sharedExecutor = new TezSharedExecutor(conf);
  }

  @AfterAll
  public static void cleanup() {
    sharedExecutor.shutdownNow();
  }

  private InputContext createInputContext() throws IOException {
    DataOutputBuffer portDob = new DataOutputBuffer();
    portDob.writeInt(PORT);
    final ByteBuffer shuffleMetaData = ByteBuffer.wrap(portDob.getData(), 0, portDob.getLength());
    portDob.close();

    ExecutionContext executionContext = mock(ExecutionContext.class);
    doReturn(FETCHER_HOST).when(executionContext).getHostName();

    InputContext inputContext = mock(InputContext.class);
    doReturn(new TezCounters()).when(inputContext).getCounters();
    doReturn(shuffleMetaData).when(inputContext)
        .getServiceProviderMetaData(conf.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
            TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT));
    doReturn(executionContext).when(inputContext).getExecutionContext();
    doReturn("Map 1").when(inputContext).getSourceVertexName();
    doReturn("Reducer 1").when(inputContext).getTaskVertexName();
    when(inputContext.getUniqueIdentifier()).thenReturn("attempt_1685094627632_0157_1_01_000000_0_10006");
    return inputContext;
  }

  @Test
  @Timeout(value = 50000, unit = TimeUnit.MILLISECONDS)
  public void testUseSharedExecutor() throws Exception {
    try (MockedStatic<ShuffleUtils> shuffleUtils = Mockito.mockStatic(ShuffleUtils.class)) {
      shuffleUtils.when(() -> ShuffleUtils.deserializeShuffleProviderMetaData(any())).thenReturn(4);
      shuffleUtils.when(() -> ShuffleUtils.getHttpConnectionParams(any())).thenReturn(
          new HttpConnectionParams(false, 1000, 5000, 1000, 104857600, false, null));

      try (MockedStatic<UmbilicalUtils> umbilicalUtils = Mockito.mockStatic(UmbilicalUtils.class)) {
        Map<Integer, List<ShuffleServerInfo>> workers = new HashMap<>();
        workers.put(1, ImmutableList.of(new ShuffleServerInfo("127.0.0.1", 2181)));
        umbilicalUtils.when(() -> UmbilicalUtils.requestShuffleServer(any(), any(), any(), anyInt()))
            .thenReturn(workers);

        InputContext inputContext = createInputContext();
        createShuffleManager(inputContext, 2);
        verify(inputContext, times(0)).createTezFrameworkExecutorService(anyInt(), anyString());
      }
    }
  }

  @Test
  @Timeout(value = 20000, unit = TimeUnit.MILLISECONDS)
  public void testProgressWithEmptyPendingHosts() throws Exception {
    try (MockedStatic<ShuffleUtils> shuffleUtils = Mockito.mockStatic(ShuffleUtils.class)) {
      shuffleUtils.when(() -> ShuffleUtils.deserializeShuffleProviderMetaData(any())).thenReturn(4);
      HttpConnectionParams params = new HttpConnectionParams(false, 1000, 5000,
          1000, 1024 * 1024 * 100, false, null);
      shuffleUtils.when(() -> ShuffleUtils.getHttpConnectionParams(any())).thenReturn(params);
      InputContext inputContext = createInputContext();
      final ShuffleManager shuffleManager = spy(createShuffleManager(inputContext, 1));
      Thread schedulerGetHostThread = new Thread(new Runnable() {
        @Override
        public void run() {
          try (MockedStatic<UmbilicalUtils> umbilicalUtils = Mockito.mockStatic(UmbilicalUtils.class)) {
            Map<Integer, List<ShuffleServerInfo>> workers = new HashMap<>();
            workers.put(1, ImmutableList.of(new ShuffleServerInfo("127.0.0.1", 2181)));
            umbilicalUtils.when(() -> UmbilicalUtils.requestShuffleServer(any(), any(), any(), anyInt()))
                .thenReturn(workers);
            try {
              shuffleManager.run();
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      });
      schedulerGetHostThread.start();
      Thread.currentThread().sleep(1000 * 3 + 1000);
      schedulerGetHostThread.interrupt();
      verify(inputContext, atLeast(3)).notifyProgress();
    }
  }

  @Test
  @Timeout(value = 2000000, unit = TimeUnit.MILLISECONDS)
  public void testFetchFailed() throws Exception {
    try (MockedStatic<ShuffleUtils> shuffleUtils = Mockito.mockStatic(ShuffleUtils.class)) {
      shuffleUtils.when(() -> ShuffleUtils.deserializeShuffleProviderMetaData(any())).thenReturn(4);
      shuffleUtils.when(() -> ShuffleUtils.getHttpConnectionParams(any())).thenReturn(
          new HttpConnectionParams(false, 1000, 5000, 1000, 1024 * 1024 * 100, false, null));

      InputContext inputContext = createInputContext();
      final ShuffleManager shuffleManager = spy(createShuffleManager(inputContext, 1));
      Thread schedulerGetHostThread = new Thread(new Runnable() {
        @Override
        public void run() {
          try (MockedStatic<UmbilicalUtils> umbilicalUtils = Mockito.mockStatic(UmbilicalUtils.class)) {
            Map<Integer, List<ShuffleServerInfo>> workers = new HashMap<>();
            workers.put(1, ImmutableList.of(new ShuffleServerInfo("127.0.0.1", 2181)));
            umbilicalUtils.when(() -> UmbilicalUtils.requestShuffleServer(any(), any(), any(), anyInt()))
                .thenReturn(workers);
            try {
              shuffleManager.run();
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      });
      InputAttemptIdentifier inputAttemptIdentifier  = new InputAttemptIdentifier(1, 1);

      schedulerGetHostThread.start();
      Thread.sleep(1000);
      shuffleManager.fetchFailed("host1", inputAttemptIdentifier, false);
      Thread.sleep(1000);

      ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
      verify(inputContext, times(1))
          .sendEvents(captor.capture());
      assertEquals(captor.getAllValues().size(), 1);
      List<Event> capturedList = captor.getAllValues().get(0);
      assertEquals(capturedList.size(), 1);
      InputReadErrorEvent inputEvent = (InputReadErrorEvent) capturedList.get(0);

      shuffleManager.fetchFailed("host1", inputAttemptIdentifier, false);
      shuffleManager.fetchFailed("host1", inputAttemptIdentifier, false);

      // Wait more than five seconds for the batch to go out
      Thread.sleep(5000);
      captor = ArgumentCaptor.forClass(List.class);
      assertEquals(capturedList.size(), 1);
    }
  }

  private ShuffleManagerForTest createShuffleManager(
      InputContext inputContext, int expectedNumOfPhysicalInputs)
      throws IOException {
    Path outDirBase = new Path(".", "outDir");
    String[] outDirs = new String[] { outDirBase.toString() };
    doReturn(outDirs).when(inputContext).getWorkDirs();
    conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, inputContext.getWorkDirs());

    DataOutputBuffer out = new DataOutputBuffer();
    Token<JobTokenIdentifier> token = new Token<JobTokenIdentifier>(new JobTokenIdentifier(),
        new JobTokenSecretManager(null));
    token.write(out);
    doReturn(ByteBuffer.wrap(out.getData())).when(inputContext)
        .getServiceConsumerMetaData(
            conf.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
                TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT));

    FetchedInputAllocator inputAllocator = mock(FetchedInputAllocator.class);
    return new ShuffleManagerForTest(inputContext, conf,
        expectedNumOfPhysicalInputs, 1024, false, -1, null, inputAllocator);
  }

  private Event createDataMovementEvent(String host, int srcIndex, int targetIndex) {
    DataMovementEventPayloadProto.Builder builder = DataMovementEventPayloadProto.newBuilder();
    builder.setHost(host);
    builder.setPort(PORT);
    builder.setPathComponent(PATH_COMPONENT);
    Event dme = DataMovementEvent.create(srcIndex, targetIndex, 0,
        builder.build().toByteString().asReadOnlyByteBuffer());
    return dme;
  }

  private static class ShuffleManagerForTest extends RssShuffleManager {
    ShuffleManagerForTest(InputContext inputContext, Configuration conf, int numInputs, int bufferSize,
          boolean ifileReadAheadEnabled, int ifileReadAheadLength, CompressionCodec codec,
          FetchedInputAllocator inputAllocator) throws IOException {
      super(inputContext, conf, numInputs, bufferSize, ifileReadAheadEnabled,
          ifileReadAheadLength, codec, inputAllocator, 0);
    }

    @Override
    Fetcher constructFetcherForHost(InputHost inputHost, Configuration conf) {
      final Fetcher fetcher = spy(super.constructFetcherForHost(inputHost, conf));
      final FetchResult mockFetcherResult = mock(FetchResult.class);
      try {
        doAnswer(new Answer<FetchResult>() {
          @Override
          public FetchResult answer(InvocationOnMock invocation) throws Throwable {
            for (InputAttemptIdentifier input : fetcher.getSrcAttempts()) {
              ShuffleManagerForTest.this.fetchSucceeded(
                  fetcher.getHost(), input, new TestFetchedInput(input), 0, 0,
                  0);
            }
            return mockFetcherResult;
          }
        }).when(fetcher).callInternal();
      } catch (Exception e) {
        //ignore
      }
      return fetcher;
    }

    public int getNumOfCompletedInputs() {
      return completedInputSet.cardinality();
    }

    boolean isFetcherExecutorShutdown() {
      return fetcherExecutor.isShutdown();
    }
  }

  /**
   * Fake input that is added to the completed input list in case an input does not have any data.
   *
   */
  @VisibleForTesting
  static class TestFetchedInput extends FetchedInput {

    TestFetchedInput(InputAttemptIdentifier inputAttemptIdentifier) {
      super(inputAttemptIdentifier, null);
    }

    @Override
    public long getSize() {
      return -1;
    }

    @Override
    public Type getType() {
      return Type.MEMORY;
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
      return null;
    }

    @Override
    public InputStream getInputStream() throws IOException {
      return null;
    }

    @Override
    public void commit() throws IOException {
    }

    @Override
    public void abort() throws IOException {
    }

    @Override
    public void free() {
    }
  }
}
