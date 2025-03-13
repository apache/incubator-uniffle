package org.apache.spark.shuffle.handle.split;

import org.apache.uniffle.common.PartitionSplitMode;
import org.apache.uniffle.common.ShuffleServerInfo;

import java.util.List;

public class PartitionSplitInfo {
    private int partitionId;
    private boolean isSplit;
    private PartitionSplitMode mode;

    // first list's index is the replica index
    // nested list is the all assigned shuffle-servers
    private List<List<ShuffleServerInfo>> splitServers;

    public PartitionSplitInfo(int partitionId, boolean isSplit, PartitionSplitMode mode, List<List<ShuffleServerInfo>> splitServers) {
        this.partitionId = partitionId;
        this.isSplit = isSplit;
        this.mode = mode;
        this.splitServers = splitServers;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public boolean isSplit() {
        return isSplit;
    }

    public PartitionSplitMode getMode() {
        return mode;
    }

    public List<List<ShuffleServerInfo>> getSplitServers() {
        return splitServers;
    }
}
