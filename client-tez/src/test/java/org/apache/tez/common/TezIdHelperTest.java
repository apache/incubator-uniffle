package org.apache.tez.common;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TezIdHelperTest {

  @Test
  public void TestTetTaskAttemptId(){
    TezIdHelper tezIdHelper = new TezIdHelper();
    assertEquals(0, tezIdHelper.getTaskAttemptId(27262976));
    assertEquals(1, tezIdHelper.getTaskAttemptId(27262977));
    assertEquals(0, RssTezUtils.taskIdStrToTaskId("attempt_1680867852986_0012_1_01_000000_0_10003"));
    assertEquals(tezIdHelper.getTaskAttemptId(27262976), RssTezUtils.taskIdStrToTaskId("attempt_1680867852986_0012_1_01_000000_0_10003"));
  }
}
