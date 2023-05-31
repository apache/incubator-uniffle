package org.apache.tez.common;

import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.InputContext;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InputContextUtilsTest {

  @Test
  public void testGetTezTaskAttemptID() {
    InputContext inputContext = mock(InputContext.class);
    when(inputContext.getUniqueIdentifier()).thenReturn("attempt_1685094627632_0157_1_01_000000_0_10006");

    TezTaskAttemptID rightTaskAttemptID = TezTaskAttemptID.fromString("attempt_1685094627632_0157_1_01_000000_0");
    assertEquals(rightTaskAttemptID, InputContextUtils.getTezTaskAttemptID(inputContext));
  }


  @Test
  public void testComputeShuffleId() {
    InputContext inputContext = mock(InputContext.class);
    when(inputContext.getDagIdentifier()).thenReturn(1);
    when(inputContext.getSourceVertexName()).thenReturn("Map 1");
    when(inputContext.getTaskVertexName()).thenReturn("Reducer 1");

    assertEquals(1001601, InputContextUtils.computeShuffleId(inputContext));
  }
}
