package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;


import java.io.IOException;
import java.util.Arrays;

import com.google.common.primitives.Ints;
import org.apache.uniffle.common.util.ChecksumUtils;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RssTezBypassWriterTest {
  @Test
  public void testWrite(){
  }


  @Test
  public void testCalcChecksum() throws IOException {
    byte[] data = new byte[]{1, 2, -1, 1, 2, -1, -1};
    byte[] result = new byte[]{-71, -87, 19, -71};
    assertTrue(Arrays.equals(Ints.toByteArray((int)ChecksumUtils.getCrc32(data)), result));
  }
}
