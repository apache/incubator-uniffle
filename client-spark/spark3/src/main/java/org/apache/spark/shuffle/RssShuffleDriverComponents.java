package org.apache.spark.shuffle;

import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleDriverComponents;

public class RssShuffleDriverComponents extends LocalDiskShuffleDriverComponents {

    private final SparkConf sparkConf;

    public RssShuffleDriverComponents(SparkConf sparkConf) {
        this.sparkConf = sparkConf;
    }

    /**
     * 1.Omitting @Override annotation to avoid compile error before Spark 3.5.0
     *
     * 2.This method is called after DelegationRssShuffleManager initialize, so RssSparkConfig.RSS_ENABLED must be already set
     */
    public boolean supportsReliableStorage() {
        return sparkConf.get(RssSparkConfig.RSS_ENABLED);
    }
}
