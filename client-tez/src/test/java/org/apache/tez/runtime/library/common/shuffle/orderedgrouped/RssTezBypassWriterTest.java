package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;


import java.io.IOException;
import java.util.Arrays;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RssTezBypassWriterTest {

  @Test
  public void testCalcChecksum() throws IOException {
    byte[] data = new byte[]{1, 2, -1, 1, 2, -1, -1};
    byte[] checksum = RssTezBypassWriter.calcChecksum(data);
    assertTrue(Arrays.equals(new byte[]{-71, -87, 19, -71}, checksum));
  }
}
