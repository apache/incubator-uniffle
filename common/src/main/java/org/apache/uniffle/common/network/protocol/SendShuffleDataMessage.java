package org.apache.uniffle.common.network.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.util.MessageConstants;

import java.util.List;
import java.util.Map;

public class SendShuffleDataMessage implements RequestMessage {
  public long requestId;
  private String appId;
  private int shuffleId;
  private long requireId;
  private Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks;
  private int size = -1;

  public SendShuffleDataMessage(String appId, int shuffleId, long requireId, Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.requireId = requireId;
    this.partitionToBlocks = partitionToBlocks;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public long getRequireId() {
    return requireId;
  }

  public Map<Integer, List<ShuffleBlockInfo>> getPartitionToBlocks() {
    return partitionToBlocks;
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.ensureWritable(size());
    byte[] appIdBytes = appId.getBytes();
    // write length of appId and appId
    buf.writeBytes(new byte[]{MessageConstants.UPLOAD_DATA_MAGIC_BYTE});
    buf.writeLong(requestId);
    buf.writeInt(appId.length());
    buf.writeBytes(appIdBytes);
    buf.writeInt(shuffleId);
    buf.writeLong(requireId);
    uploadData(buf);
  }

  private void uploadData(ByteBuf buf) {
    for (Map.Entry<Integer, List<ShuffleBlockInfo>> entry : partitionToBlocks.entrySet()) {
      // write partitionId
      buf.writeInt(entry.getKey());
      for (ShuffleBlockInfo sbi : entry.getValue()) {
        buf.writeBytes(new byte[]{MessageConstants.MESSAGE_UPLOAD_DATA_PARTITION_CONTINUE});
        // write blockInfo include blockId, crc, uncompressLength, length
        buf.writeLong(sbi.getBlockId());
        buf.writeLong(sbi.getCrc());
        buf.writeInt(sbi.getUncompressLength());
        buf.writeInt(sbi.getLength());
        buf.writeLong(sbi.getTaskAttemptId());
        buf.writeBytes(sbi.getData());
      }
      // write flag which means no block for current partitionId
      buf.writeBytes(new byte[]{MessageConstants.MESSAGE_UPLOAD_DATA_PARTITION_END});
    }
    // write flag which means no block for current shuffleId
    buf.writeInt(MessageConstants.MESSAGE_UPLOAD_DATA_END);
  }


  @Override
  public long getRequestId() {
    return requestId;
  }

  @Override
  public int size() {
    if (size == -1) {
      size = calcSize();
    }
    return size;
  }

  private int calcSize() {
    // magic + requestId + appId length + appId + shuffleId + requireId
    int shuffleHeader =  1 + 8 + 4 + appId.length() + 4 + 8;
    int partitionDataSize = 0;
    for(Map.Entry<Integer, List<ShuffleBlockInfo>> entry : partitionToBlocks.entrySet()) {
      // partitionId
      partitionDataSize += 4;
      for(ShuffleBlockInfo block : entry.getValue()) {
        // block header 32 bytes + block length + continue flag
        partitionDataSize += 32 + block.getLength() + 1;
      }
      // partition end flag
      partitionDataSize += 1;
    }
    // data end flag
    partitionDataSize += 4;
    return shuffleHeader + partitionDataSize;
  }

}
