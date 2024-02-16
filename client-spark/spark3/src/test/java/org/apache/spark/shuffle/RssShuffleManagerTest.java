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

package org.apache.spark.shuffle;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.client.util.RssClientConfig;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.spark.shuffle.RssSparkConfig.RSS_SHUFFLE_MANAGER_GRPC_PORT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RssShuffleManagerTest extends RssShuffleManagerTestBase {
  private static final String SPARK_ADAPTIVE_EXECUTION_ENABLED_KEY = "spark.sql.adaptive.enabled";

  @Test
  public void testGetDataDistributionType() {
    // case1
    SparkConf sparkConf = new SparkConf();
    sparkConf.set(SPARK_ADAPTIVE_EXECUTION_ENABLED_KEY, "true");
    assertEquals(
        ShuffleDataDistributionType.LOCAL_ORDER,
        RssShuffleManager.getDataDistributionType(sparkConf));

    // case2
    sparkConf = new SparkConf();
    sparkConf.set(SPARK_ADAPTIVE_EXECUTION_ENABLED_KEY, "false");
    assertEquals(
        RssClientConf.DATA_DISTRIBUTION_TYPE.defaultValue(),
        RssShuffleManager.getDataDistributionType(sparkConf));

    // case3
    sparkConf = new SparkConf();
    sparkConf.set(SPARK_ADAPTIVE_EXECUTION_ENABLED_KEY, "true");
    sparkConf.set(
        "spark." + RssClientConf.DATA_DISTRIBUTION_TYPE.key(),
        ShuffleDataDistributionType.NORMAL.name());
    assertEquals(
        ShuffleDataDistributionType.NORMAL, RssShuffleManager.getDataDistributionType(sparkConf));

    // case4
    sparkConf = new SparkConf();
    sparkConf.set(SPARK_ADAPTIVE_EXECUTION_ENABLED_KEY, "true");
    sparkConf.set(
        "spark." + RssClientConf.DATA_DISTRIBUTION_TYPE.key(),
        ShuffleDataDistributionType.LOCAL_ORDER.name());
    assertEquals(
        ShuffleDataDistributionType.LOCAL_ORDER,
        RssShuffleManager.getDataDistributionType(sparkConf));

    // case5
    sparkConf = new SparkConf();
    boolean aqeEnable = (boolean) sparkConf.get(SQLConf.ADAPTIVE_EXECUTION_ENABLED());
    if (aqeEnable) {
      assertEquals(
          ShuffleDataDistributionType.LOCAL_ORDER,
          RssShuffleManager.getDataDistributionType(sparkConf));
    } else {
      assertEquals(
          RssClientConf.DATA_DISTRIBUTION_TYPE.defaultValue(),
          RssShuffleManager.getDataDistributionType(sparkConf));
    }
  }

  private long bits(String string) {
    return Long.parseLong(string.replaceAll("[|]", ""), 2);
  }

  @Test
  public void testGetTaskAttemptIdWithoutSpeculation() {
    // the expected bits("xy|z") represents the expected Long in bit notation where | is used to
    // separate
    // map index from attempt number, so merely for visualization purposes

    // maxFailures < 1 not allowed, but we fall back to maxFailures=1 to be safe against that user
    // input
    for (int maxFailures : Arrays.asList(-1, 0, 1)) {
      assertEquals(
          bits("0000|"),
          RssShuffleManager.getTaskAttemptId(0, 0, maxFailures, false, 10),
          String.valueOf(maxFailures));
      assertEquals(
          bits("0001|"),
          RssShuffleManager.getTaskAttemptId(1, 0, maxFailures, false, 10),
          String.valueOf(maxFailures));
      assertEquals(
          bits("0010|"),
          RssShuffleManager.getTaskAttemptId(2, 0, maxFailures, false, 10),
          String.valueOf(maxFailures));
    }

    // maxFailures of 2
    assertEquals(bits("000|0"), RssShuffleManager.getTaskAttemptId(0, 0, 2, false, 10));
    assertEquals(bits("000|1"), RssShuffleManager.getTaskAttemptId(0, 1, 2, false, 10));
    assertEquals(bits("001|0"), RssShuffleManager.getTaskAttemptId(1, 0, 2, false, 10));
    assertEquals(bits("001|1"), RssShuffleManager.getTaskAttemptId(1, 1, 2, false, 10));
    assertEquals(bits("010|0"), RssShuffleManager.getTaskAttemptId(2, 0, 2, false, 10));
    assertEquals(bits("010|1"), RssShuffleManager.getTaskAttemptId(2, 1, 2, false, 10));
    assertEquals(bits("011|0"), RssShuffleManager.getTaskAttemptId(3, 0, 2, false, 10));
    assertEquals(bits("011|1"), RssShuffleManager.getTaskAttemptId(3, 1, 2, false, 10));

    // maxFailures of 3
    assertEquals(bits("00|00"), RssShuffleManager.getTaskAttemptId(0, 0, 3, false, 10));
    assertEquals(bits("00|01"), RssShuffleManager.getTaskAttemptId(0, 1, 3, false, 10));
    assertEquals(bits("00|10"), RssShuffleManager.getTaskAttemptId(0, 2, 3, false, 10));
    assertEquals(bits("01|00"), RssShuffleManager.getTaskAttemptId(1, 0, 3, false, 10));
    assertEquals(bits("01|01"), RssShuffleManager.getTaskAttemptId(1, 1, 3, false, 10));
    assertEquals(bits("01|10"), RssShuffleManager.getTaskAttemptId(1, 2, 3, false, 10));
    assertEquals(bits("10|00"), RssShuffleManager.getTaskAttemptId(2, 0, 3, false, 10));
    assertEquals(bits("10|01"), RssShuffleManager.getTaskAttemptId(2, 1, 3, false, 10));
    assertEquals(bits("10|10"), RssShuffleManager.getTaskAttemptId(2, 2, 3, false, 10));
    assertEquals(bits("11|00"), RssShuffleManager.getTaskAttemptId(3, 0, 3, false, 10));
    assertEquals(bits("11|01"), RssShuffleManager.getTaskAttemptId(3, 1, 3, false, 10));
    assertEquals(bits("11|10"), RssShuffleManager.getTaskAttemptId(3, 2, 3, false, 10));

    // maxFailures of 4
    assertEquals(bits("00|00"), RssShuffleManager.getTaskAttemptId(0, 0, 4, false, 10));
    assertEquals(bits("00|01"), RssShuffleManager.getTaskAttemptId(0, 1, 4, false, 10));
    assertEquals(bits("00|10"), RssShuffleManager.getTaskAttemptId(0, 2, 4, false, 10));
    assertEquals(bits("00|11"), RssShuffleManager.getTaskAttemptId(0, 3, 4, false, 10));
    assertEquals(bits("01|00"), RssShuffleManager.getTaskAttemptId(1, 0, 4, false, 10));
    assertEquals(bits("01|01"), RssShuffleManager.getTaskAttemptId(1, 1, 4, false, 10));
    assertEquals(bits("01|10"), RssShuffleManager.getTaskAttemptId(1, 2, 4, false, 10));
    assertEquals(bits("01|11"), RssShuffleManager.getTaskAttemptId(1, 3, 4, false, 10));
    assertEquals(bits("10|00"), RssShuffleManager.getTaskAttemptId(2, 0, 4, false, 10));
    assertEquals(bits("10|01"), RssShuffleManager.getTaskAttemptId(2, 1, 4, false, 10));
    assertEquals(bits("10|10"), RssShuffleManager.getTaskAttemptId(2, 2, 4, false, 10));
    assertEquals(bits("10|11"), RssShuffleManager.getTaskAttemptId(2, 3, 4, false, 10));
    assertEquals(bits("11|00"), RssShuffleManager.getTaskAttemptId(3, 0, 4, false, 10));
    assertEquals(bits("11|01"), RssShuffleManager.getTaskAttemptId(3, 1, 4, false, 10));
    assertEquals(bits("11|10"), RssShuffleManager.getTaskAttemptId(3, 2, 4, false, 10));
    assertEquals(bits("11|11"), RssShuffleManager.getTaskAttemptId(3, 3, 4, false, 10));

    // maxFailures of 5
    assertEquals(bits("0|000"), RssShuffleManager.getTaskAttemptId(0, 0, 5, false, 10));
    assertEquals(bits("1|100"), RssShuffleManager.getTaskAttemptId(1, 4, 5, false, 10));

    // test with ints that overflow into signed int and long
    assertEquals(
        Integer.MAX_VALUE, RssShuffleManager.getTaskAttemptId(Integer.MAX_VALUE, 0, 1, false, 31));
    assertEquals(
        (long) Integer.MAX_VALUE << 1 | 1,
        RssShuffleManager.getTaskAttemptId(Integer.MAX_VALUE, 1, 2, false, 32));
    assertEquals(
        (long) Integer.MAX_VALUE << 2 | 3,
        RssShuffleManager.getTaskAttemptId(Integer.MAX_VALUE, 3, 4, false, 33));
    assertEquals(
        (long) Integer.MAX_VALUE << 3 | 7,
        RssShuffleManager.getTaskAttemptId(Integer.MAX_VALUE, 7, 8, false, 34));

    // test with attemptNo >= maxFailures
    assertThrowsExactly(
        RssException.class, () -> RssShuffleManager.getTaskAttemptId(0, 1, -1, false, 10));
    assertThrowsExactly(
        RssException.class, () -> RssShuffleManager.getTaskAttemptId(0, 1, 0, false, 10));
    for (int maxFailures : Arrays.asList(1, 2, 3, 4, 8, 128)) {
      assertThrowsExactly(
          RssException.class,
          () -> RssShuffleManager.getTaskAttemptId(0, maxFailures, maxFailures, false, 10),
          String.valueOf(maxFailures));
      assertThrowsExactly(
          RssException.class,
          () -> RssShuffleManager.getTaskAttemptId(0, maxFailures + 1, maxFailures, false, 10),
          String.valueOf(maxFailures));
      assertThrowsExactly(
          RssException.class,
          () -> RssShuffleManager.getTaskAttemptId(0, maxFailures + 2, maxFailures, false, 10),
          String.valueOf(maxFailures));
      Exception e =
          assertThrowsExactly(
              RssException.class,
              () ->
                  RssShuffleManager.getTaskAttemptId(0, maxFailures + 128, maxFailures, false, 10),
              String.valueOf(maxFailures));
      assertEquals(
          "Observing attempt number "
              + (maxFailures + 128)
              + " while spark.task.maxFailures is set to "
              + maxFailures
              + ".",
          e.getMessage());
    }

    // test with mapIndex that would require more than maxTaskAttemptBits
    Exception e =
        assertThrowsExactly(
            RssException.class, () -> RssShuffleManager.getTaskAttemptId(256, 0, 3, true, 10));
    assertEquals(
        "Observing mapIndex[256] that would produce a taskAttemptId with 11 bits "
            + "which is larger than the allowed 10 bits (maxFailures[3], speculation[true]).",
        e.getMessage());
    // check that a lower mapIndex works as expected
    assertEquals(bits("11111111|00"), RssShuffleManager.getTaskAttemptId(255, 0, 3, true, 10));
  }

  @Test
  public void testGetTaskAttemptIdWithSpeculation() {
    // with speculation, we expect maxFailures+1 attempts

    // the expected bits("xy|z") represents the expected Long in bit notation where | is used to
    // separate
    // map index from attempt number, so merely for visualization purposes

    // maxFailures < 1 not allowed, but we fall back to maxFailures=1 to be safe against that user
    // input
    for (int maxFailures : Arrays.asList(-1, 0, 1)) {
      for (int attemptNo : Arrays.asList(0, 1)) {
        assertEquals(
            bits("0000|" + attemptNo),
            RssShuffleManager.getTaskAttemptId(0, attemptNo, maxFailures, true, 10),
            "maxFailures=" + maxFailures + ", attemptNo=" + attemptNo);
        assertEquals(
            bits("0001|" + attemptNo),
            RssShuffleManager.getTaskAttemptId(1, attemptNo, maxFailures, true, 10),
            "maxFailures=" + maxFailures + ", attemptNo=" + attemptNo);
        assertEquals(
            bits("0010|" + attemptNo),
            RssShuffleManager.getTaskAttemptId(2, attemptNo, maxFailures, true, 10),
            "maxFailures=" + maxFailures + ", attemptNo=" + attemptNo);
      }
    }

    // maxFailures of 2
    assertEquals(bits("00|00"), RssShuffleManager.getTaskAttemptId(0, 0, 2, true, 10));
    assertEquals(bits("00|01"), RssShuffleManager.getTaskAttemptId(0, 1, 2, true, 10));
    assertEquals(bits("00|10"), RssShuffleManager.getTaskAttemptId(0, 2, 2, true, 10));
    assertEquals(bits("01|00"), RssShuffleManager.getTaskAttemptId(1, 0, 2, true, 10));
    assertEquals(bits("01|01"), RssShuffleManager.getTaskAttemptId(1, 1, 2, true, 10));
    assertEquals(bits("01|10"), RssShuffleManager.getTaskAttemptId(1, 2, 2, true, 10));
    assertEquals(bits("10|00"), RssShuffleManager.getTaskAttemptId(2, 0, 2, true, 10));
    assertEquals(bits("10|01"), RssShuffleManager.getTaskAttemptId(2, 1, 2, true, 10));
    assertEquals(bits("10|10"), RssShuffleManager.getTaskAttemptId(2, 2, 2, true, 10));
    assertEquals(bits("11|00"), RssShuffleManager.getTaskAttemptId(3, 0, 2, true, 10));
    assertEquals(bits("11|01"), RssShuffleManager.getTaskAttemptId(3, 1, 2, true, 10));
    assertEquals(bits("11|10"), RssShuffleManager.getTaskAttemptId(3, 2, 2, true, 10));

    // maxFailures of 3
    assertEquals(bits("00|00"), RssShuffleManager.getTaskAttemptId(0, 0, 3, true, 10));
    assertEquals(bits("00|01"), RssShuffleManager.getTaskAttemptId(0, 1, 3, true, 10));
    assertEquals(bits("00|10"), RssShuffleManager.getTaskAttemptId(0, 2, 3, true, 10));
    assertEquals(bits("00|11"), RssShuffleManager.getTaskAttemptId(0, 3, 3, true, 10));
    assertEquals(bits("01|00"), RssShuffleManager.getTaskAttemptId(1, 0, 3, true, 10));
    assertEquals(bits("01|01"), RssShuffleManager.getTaskAttemptId(1, 1, 3, true, 10));
    assertEquals(bits("01|10"), RssShuffleManager.getTaskAttemptId(1, 2, 3, true, 10));
    assertEquals(bits("01|11"), RssShuffleManager.getTaskAttemptId(1, 3, 3, true, 10));
    assertEquals(bits("10|00"), RssShuffleManager.getTaskAttemptId(2, 0, 3, true, 10));
    assertEquals(bits("10|01"), RssShuffleManager.getTaskAttemptId(2, 1, 3, true, 10));
    assertEquals(bits("10|10"), RssShuffleManager.getTaskAttemptId(2, 2, 3, true, 10));
    assertEquals(bits("10|11"), RssShuffleManager.getTaskAttemptId(2, 3, 3, true, 10));
    assertEquals(bits("11|00"), RssShuffleManager.getTaskAttemptId(3, 0, 3, true, 10));
    assertEquals(bits("11|01"), RssShuffleManager.getTaskAttemptId(3, 1, 3, true, 10));
    assertEquals(bits("11|10"), RssShuffleManager.getTaskAttemptId(3, 2, 3, true, 10));
    assertEquals(bits("11|11"), RssShuffleManager.getTaskAttemptId(3, 3, 3, true, 10));

    // maxFailures of 4
    assertEquals(bits("0|000"), RssShuffleManager.getTaskAttemptId(0, 0, 4, true, 10));
    assertEquals(bits("1|100"), RssShuffleManager.getTaskAttemptId(1, 4, 4, true, 10));

    // test with ints that overflow into signed int and long
    assertEquals(
        (long) Integer.MAX_VALUE << 1,
        RssShuffleManager.getTaskAttemptId(Integer.MAX_VALUE, 0, 1, true, 32));
    assertEquals(
        (long) Integer.MAX_VALUE << 1 | 1,
        RssShuffleManager.getTaskAttemptId(Integer.MAX_VALUE, 1, 1, true, 32));
    assertEquals(
        (long) Integer.MAX_VALUE << 2 | 3,
        RssShuffleManager.getTaskAttemptId(Integer.MAX_VALUE, 3, 3, true, 33));
    assertEquals(
        (long) Integer.MAX_VALUE << 3 | 7,
        RssShuffleManager.getTaskAttemptId(Integer.MAX_VALUE, 7, 7, true, 34));

    // test with attemptNo > maxFailures (attemptNo == maxFailures allowed for speculation enabled)
    assertThrowsExactly(
        RssException.class, () -> RssShuffleManager.getTaskAttemptId(0, 2, -1, true, 10));
    assertThrowsExactly(
        RssException.class, () -> RssShuffleManager.getTaskAttemptId(0, 2, 0, true, 10));
    for (int maxFailures : Arrays.asList(1, 2, 3, 4, 8, 128)) {
      assertThrowsExactly(
          RssException.class,
          () -> RssShuffleManager.getTaskAttemptId(0, maxFailures + 1, maxFailures, true, 10),
          String.valueOf(maxFailures));
      assertThrowsExactly(
          RssException.class,
          () -> RssShuffleManager.getTaskAttemptId(0, maxFailures + 2, maxFailures, true, 10),
          String.valueOf(maxFailures));
      Exception e =
          assertThrowsExactly(
              RssException.class,
              () -> RssShuffleManager.getTaskAttemptId(0, maxFailures + 128, maxFailures, true, 10),
              String.valueOf(maxFailures));
      assertEquals(
          "Observing attempt number "
              + (maxFailures + 128)
              + " while spark.task.maxFailures is set to "
              + maxFailures
              + " with speculation enabled.",
          e.getMessage());
    }

    // test with mapIndex that would require more than maxTaskAttemptBits
    Exception e =
        assertThrowsExactly(
            RssException.class, () -> RssShuffleManager.getTaskAttemptId(256, 0, 4, false, 10));
    assertEquals(
        "Observing mapIndex[256] that would produce a taskAttemptId with 11 bits "
            + "which is larger than the allowed 10 bits (maxFailures[4], speculation[false]).",
        e.getMessage());
    // check that a lower mapIndex works as expected
    assertEquals(bits("11111111|00"), RssShuffleManager.getTaskAttemptId(255, 0, 4, false, 10));
  }

  @Test
  public void testCreateShuffleManagerServer() {
    setupMockedRssShuffleUtils(StatusCode.SUCCESS);

    SparkConf conf = new SparkConf();
    conf.set(RssSparkConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED.key(), "false");
    conf.set(RssSparkConfig.RSS_COORDINATOR_QUORUM.key(), "m1:8001,m2:8002");
    conf.set("spark.rss.storage.type", StorageType.LOCALFILE.name());
    conf.set(RssSparkConfig.RSS_TEST_MODE_ENABLE, true);
    // enable stage recompute
    conf.set("spark." + RssClientConfig.RSS_RESUBMIT_STAGE, "true");

    RssShuffleManager shuffleManager = new RssShuffleManager(conf, true);

    assertTrue(conf.get(RSS_SHUFFLE_MANAGER_GRPC_PORT) > 0);
  }

  @Test
  public void testRssShuffleManagerInterface() throws Exception {
    setupMockedRssShuffleUtils(StatusCode.SUCCESS);

    SparkConf conf = new SparkConf();
    conf.set(RssSparkConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED.key(), "false");
    conf.set(RssSparkConfig.RSS_COORDINATOR_QUORUM.key(), "m1:8001,m2:8002");
    conf.set("spark.rss.storage.type", StorageType.LOCALFILE.name());
    conf.set(RssSparkConfig.RSS_TEST_MODE_ENABLE, true);

    conf.set("spark.task.maxFailures", "3");
    RssShuffleManager shuffleManager = new RssShuffleManager(conf, true);
    assertEquals(shuffleManager.getMaxFetchFailures(), 2);
    // by default, the appId is null
    assertNull(shuffleManager.getAppId());
  }
}
