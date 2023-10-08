package org.apache.spark.shuffle;

import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.api.ShuffleDataIO;
import org.apache.spark.shuffle.api.ShuffleDriverComponents;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleExecutorComponents;

public class RssShuffleDataIo implements ShuffleDataIO {
    private final SparkConf sparkConf;

    public RssShuffleDataIo(SparkConf sparkConf) {
        this.sparkConf = sparkConf;
    }

    /**
     * Compatible with SortShuffleManager when DelegationRssShuffleManager fallback
     */
    @Override
    public ShuffleExecutorComponents executor() {
        return new LocalDiskShuffleExecutorComponents(sparkConf);
    }

    @Override
    public ShuffleDriverComponents driver() {
        return new RssShuffleDriverComponents(sparkConf);
    }
}
