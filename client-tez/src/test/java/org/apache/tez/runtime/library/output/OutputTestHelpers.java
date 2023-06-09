package org.apache.tez.runtime.library.output;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
    doReturn(200 * 1024 * 1024l).when(ctx).getTotalMemoryAvailableToTask();
    doReturn(new TezCounters()).when(ctx).getCounters();
    OutputStatisticsReporter statsReporter = mock(OutputStatisticsReporter.class);
    doReturn(statsReporter).when(ctx).getStatisticsReporter();
    doReturn(new ExecutionContextImpl("localhost")).when(ctx).getExecutionContext();
    return ctx;
  }
}
