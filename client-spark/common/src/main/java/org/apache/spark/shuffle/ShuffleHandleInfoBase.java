package org.apache.spark.shuffle;

import java.util.List;
import java.util.Map;
import org.apache.uniffle.client.PartitionDataReplicaRequirementTracking;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleServerInfo;

public abstract class ShuffleHandleInfoBase {
    protected int shuffleId;
    protected RemoteStorageInfo remoteStorage;

    public ShuffleHandleInfoBase(int shuffleId, RemoteStorageInfo remoteStorage) {
        this.shuffleId = shuffleId;
        this.remoteStorage = remoteStorage;
    }

    public int getShuffleId() {
        return shuffleId;
    }

    public RemoteStorageInfo getRemoteStorage() {
        return remoteStorage;
    }

    public abstract Map<Integer, List<ShuffleServerInfo>> getPartitionToServers();

    public abstract PartitionDataReplicaRequirementTracking createPartitionReplicaTracking();
}
