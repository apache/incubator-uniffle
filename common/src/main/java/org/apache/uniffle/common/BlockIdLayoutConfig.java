package org.apache.uniffle.common;

import java.util.Map;

import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.util.Constants;

public class BlockIdLayoutConfig {
  // BlockId is long and consist of partitionId, taskAttemptId, atomicInt
  // the length of them are ATOMIC_INT_MAX_LENGTH + PARTITION_ID_MAX_LENGTH + TASK_ATTEMPT_ID_MAX_LENGTH = 63
  private static final int BLOCK_ID_LENGTH = 63;

  /**
   * The keys are configured in the conf of BLOCKID_LAYOUT
   */
  public static final String PARTITION_ID_LENGTH = "partitionIdLength";
  public static final String TASK_ATTEMPT_ID_LENGTH = "taskAttemptIdLength";
  public static final String SEQUENCE_ID_LENGTH = "sequenceIdLength";

  private int partitionIdLength;
  private int taskAttemptIdLength;
  private int sequenceIdLength;

  private BlockIdLayoutConfig(int partitionIdLength, int taskAttemptIdLength, int sequenceIdLength) {
    this.partitionIdLength = partitionIdLength;
    this.taskAttemptIdLength = taskAttemptIdLength;
    this.sequenceIdLength = sequenceIdLength;
  }

  public int getPartitionIdLength() {
    return partitionIdLength;
  }

  public int getTaskAttemptIdLength() {
    return taskAttemptIdLength;
  }

  public int getSequenceIdLength() {
    return sequenceIdLength;
  }

  private static BlockIdLayoutConfig from(Map<String, String> blockIdLayoutMap) {
    String partitionIdRawVal = blockIdLayoutMap.get(PARTITION_ID_LENGTH);
    if (partitionIdRawVal == null) {
      throw new IllegalArgumentException(PARTITION_ID_LENGTH + " must be configured.");
    }
    String taskAttemptIdRawVal = blockIdLayoutMap.get(TASK_ATTEMPT_ID_LENGTH);
    if (taskAttemptIdRawVal == null) {
      throw new IllegalArgumentException(TASK_ATTEMPT_ID_LENGTH + " must be configured.");
    }
    String sequenceIdRawVal = blockIdLayoutMap.get(SEQUENCE_ID_LENGTH);
    if (sequenceIdRawVal == null) {
      throw new IllegalArgumentException(SEQUENCE_ID_LENGTH + " must be configured.");
    }

    int partitionIdLength = Integer.valueOf(partitionIdRawVal);
    int taskAttemptId = Integer.valueOf(taskAttemptIdRawVal);
    int seqId = Integer.valueOf(sequenceIdRawVal);

    if (partitionIdLength + taskAttemptId + seqId != BLOCK_ID_LENGTH) {
      throw new IllegalArgumentException("The sum of all parts' length should be " + BLOCK_ID_LENGTH);
    }

    return new BlockIdLayoutConfig(partitionIdLength, taskAttemptId, seqId);
  }

  public static boolean validate(Map<String, String> blockIdLayoutMap) {
    try {
      from(blockIdLayoutMap);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public static BlockIdLayoutConfig from(RssConf rssConf) {
    Map<String, String> blockIdLayoutMap = rssConf.get(RssClientConf.BLOCKID_LAYOUT);
    if (blockIdLayoutMap == null || blockIdLayoutMap.isEmpty()) {
      return from();
    }

    return from(blockIdLayoutMap);
  }

  public static BlockIdLayoutConfig from() {
    return new BlockIdLayoutConfig(
        Constants.PARTITION_ID_MAX_LENGTH,
        Constants.TASK_ATTEMPT_ID_MAX_LENGTH,
        Constants.ATOMIC_INT_MAX_LENGTH
    );
  }

  public static BlockIdLayoutConfig from(int partitionIdLength, int taskAttemptIdLength, int sequenceIdLength) {
    if (partitionIdLength + taskAttemptIdLength + sequenceIdLength != BLOCK_ID_LENGTH) {
      throw new IllegalArgumentException("The sum of all parts' length should be " + BLOCK_ID_LENGTH);
    }
    return new BlockIdLayoutConfig(partitionIdLength, taskAttemptIdLength, sequenceIdLength);
  }

  // BlockId is long and composed of partitionId, executorId and AtomicInteger.
  // AtomicInteger is first {x} bit, max value is 2^{x} - 1
  // partitionId is next {y} bit, max value is 2^{y} - 1
  // taskAttemptId is rest of {z} bit, max value is 2^{z} - 1
  // x+y+z = 63, which are configured in BlockIdLayoutConfig.
  public static long createBlockId(
      BlockIdLayoutConfig config,
      long partitionId,
      long taskAttemptId,
      long atomicInt) {
    long maxSeqId = (1 << config.getSequenceIdLength()) - 1;
    long maxPartitionId = (1 << config.getPartitionIdLength()) - 1;
    long maxTaskAttemptId = (1 << config.getTaskAttemptIdLength() ) - 1;

    if (atomicInt < 0 || atomicInt > maxSeqId) {
      throw new IllegalArgumentException("Can't support sequence[" + atomicInt
          + "], the max value should be " + maxSeqId);
    }
    if (partitionId < 0 || partitionId > maxPartitionId) {
      throw new IllegalArgumentException("Can't support partitionId["
          + partitionId + "], the max value should be " + maxPartitionId);
    }
    if (taskAttemptId < 0 || taskAttemptId > maxTaskAttemptId) {
      throw new IllegalArgumentException("Can't support taskAttemptId["
          + taskAttemptId + "], the max value should be " + maxTaskAttemptId);
    }
    return (atomicInt << (config.getPartitionIdLength() + config.getTaskAttemptIdLength()))
        + (partitionId << config.getTaskAttemptIdLength()) + taskAttemptId;
  }
}
