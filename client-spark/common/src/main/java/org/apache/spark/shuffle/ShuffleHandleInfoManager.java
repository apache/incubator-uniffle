package org.apache.spark.shuffle;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.uniffle.common.util.JavaUtils;

public class ShuffleHandleInfoManager implements Closeable {
    private Map<Integer, ShuffleHandleInfoBase> shuffleIdToShuffleHandleInfo;

    public ShuffleHandleInfoManager() {
        this.shuffleIdToShuffleHandleInfo = JavaUtils.newConcurrentMap();
    }

    public ShuffleHandleInfoBase get(int shuffleId) {
        return shuffleIdToShuffleHandleInfo.get(shuffleId);
    }

    public void remove(int shuffleId) {
        shuffleIdToShuffleHandleInfo.remove(shuffleId);
    }

    public void register(int shuffleId, ShuffleHandleInfoBase handle) {
        shuffleIdToShuffleHandleInfo.put(shuffleId, handle);
    }

    @Override
    public void close() throws IOException {
        if (shuffleIdToShuffleHandleInfo == null) {
            return;
        }
        shuffleIdToShuffleHandleInfo.clear();
    }
}
