package org.apache.tez.runtime.library.common.sort.impl;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.common.RssTezConfig;
import org.apache.tez.common.RssTezUtils;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.common.sort.buffer.SortWriteBufferManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.ByteUnit;
import org.apache.uniffle.storage.util.StorageType;




/**{@link RssSorter} is an {@link ExternalSorter}
 * @author bin3.zhang
 */
public class RssSorter extends ExternalSorter {

    private static final Logger LOG = LoggerFactory.getLogger(RssSorter.class);
    private SortWriteBufferManager bufferManager;
    private Set<Long> successBlockIds = Sets.newConcurrentHashSet();
    private Set<Long> failedBlockIds = Sets.newConcurrentHashSet();
    private Map<Integer, List<ShuffleServerInfo>> partitionToServers;

    private int[] numRecordsPerPartition;

    /**
     * Initialization
     */
    public RssSorter(TezTaskAttemptID tezTaskAttemptID,OutputContext outputContext,
                     Configuration conf, int numMaps, int numOutputs, long initialMemoryAvailable,
                     TezTaskAttemptID tezTaskAttemptId,
                     int shuffleId,
                     Map<Integer, List<ShuffleServerInfo>> partitionToServers) throws IOException {
        super(outputContext, conf, numOutputs, initialMemoryAvailable);
        this.partitionToServers = partitionToServers;
        ApplicationId applicationId = outputContext.getApplicationId();

        LOG.info("tezTaskAttemptId is {}",tezTaskAttemptId);

        this.numRecordsPerPartition = new int[numOutputs];

        long sortmb = conf.getLong(RssTezConfig.RSS_RUNTIME_IO_SORT_MB, RssTezConfig.RSS_DEFAULT_RUNTIME_IO_SORT_MB);
        LOG.info("conf.sortmb is {}", sortmb);
        sortmb = this.availableMemoryMb;
        LOG.info("sortmb, availableMemoryMb is {}, {}", sortmb, availableMemoryMb);
        if ((sortmb & 0x7FF) != sortmb) {
            throw new IOException(
                    "Invalid \"" + RssTezConfig.RSS_RUNTIME_IO_SORT_MB + "\": " + sortmb);
        }
        double sortThreshold = conf.getDouble(RssTezConfig.RSS_CLIENT_SORT_MEMORY_USE_THRESHOLD,
                RssTezConfig.RSS_CLIENT_DEFAULT_SORT_MEMORY_USE_THRESHOLD);
        long taskAttemptId = RssTezUtils.convertTaskAttemptIdToLong(tezTaskAttemptId, 1);
        long maxSegmentSize = conf.getLong(RssTezConfig.RSS_CLIENT_MAX_BUFFER_SIZE,
                RssTezConfig.RSS_CLIENT_DEFAULT_MAX_BUFFER_SIZE);
        long maxBufferSize = conf.getLong(RssTezConfig.RSS_WRITER_BUFFER_SIZE, RssTezConfig.RSS_DEFAULT_WRITER_BUFFER_SIZE);
        double memoryThreshold = conf.getDouble(RssTezConfig.RSS_CLIENT_MEMORY_THRESHOLD,
                RssTezConfig.RSS_CLIENT_DEFAULT_MEMORY_THRESHOLD);
        double sendThreshold = conf.getDouble(RssTezConfig.RSS_CLIENT_SEND_THRESHOLD,
                RssTezConfig.RSS_CLIENT_DEFAULT_SEND_THRESHOLD);
        int batch = conf.getInt(RssTezConfig.RSS_CLIENT_BATCH_TRIGGER_NUM,
                RssTezConfig.RSS_CLIENT_DEFAULT_BATCH_TRIGGER_NUM);
        String storageType = conf.get(RssTezConfig.RSS_STORAGE_TYPE, RssTezConfig.RSS_DEFAULT_STORAGE_TYPE);
        if (StringUtils.isEmpty(storageType)) {
            throw new RssException("storage type mustn't be empty");
        }
        long sendCheckInterval = conf.getLong(RssTezConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS,
                RssTezConfig.RSS_CLIENT_DEFAULT_SEND_CHECK_INTERVAL_MS);
        long sendCheckTimeout = conf.getLong(RssTezConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS,
                RssTezConfig.RSS_CLIENT_DEFAULT_SEND_CHECK_TIMEOUT_MS);
        int bitmapSplitNum = conf.getInt(RssTezConfig.RSS_CLIENT_BITMAP_NUM, RssTezConfig.RSS_CLIENT_DEFAULT_BITMAP_NUM);

        if (conf.get(RssTezConfig.HIVE_TEZ_LOG_LEVEL, RssTezConfig.DEFAULT_HIVE_TEZ_LOG_LEVEL)
                .equalsIgnoreCase(RssTezConfig.DEBUG_HIVE_TEZ_LOG_LEVEL)) {
            LOG.info("sortmb is {}", sortmb);
            LOG.info("sortThreshold is {}", sortThreshold);
            LOG.info("taskAttemptId is {}", taskAttemptId);
            LOG.info("maxSegmentSize is {}", maxSegmentSize);
            LOG.info("maxBufferSize is {}", maxBufferSize);
            LOG.info("memoryThreshold is {}", memoryThreshold);
            LOG.info("sendThreshold is {}", sendThreshold);
            LOG.info("batch is {}", batch);
            LOG.info("storageType is {}", storageType);
            LOG.info("sendCheckInterval is {}", sendCheckInterval);
            LOG.info("sendCheckTimeout is {}", sendCheckTimeout);
            LOG.info("bitmapSplitNum is {}", bitmapSplitNum);
        }


        String containerIdStr =
                System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name());
        ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
        ApplicationAttemptId applicationAttemptId =
                containerId.getApplicationAttemptId();
        LOG.info("containerIdStr is {}", containerIdStr);
        LOG.info("containerId is {}", containerId);
        LOG.info("applicationAttemptId is {}", applicationAttemptId.toString());


        bufferManager = new SortWriteBufferManager(
                tezTaskAttemptID,
                (long)(ByteUnit.MiB.toBytes(sortmb) * sortThreshold),
                applicationAttemptId.toString(),
                taskAttemptId,
                successBlockIds,
                failedBlockIds,
                RssTezUtils.createShuffleClient(conf),
                comparator,
                maxSegmentSize,
                keySerializer,
                valSerializer,
                maxBufferSize,
                memoryThreshold,
                sendThreshold,
                batch,
                new RssConf(),
                partitionToServers,
                numMaps,
                isMemoryShuffleEnabled(storageType),
                sendCheckInterval,
                sendCheckTimeout,
                bitmapSplitNum,
                shuffleId);
        LOG.info("Initialized SortWriteBufferManager.");
    }

    @Override
    public void flush() throws IOException {
        bufferManager.waitSendFinished();
    }

    @Override
    public final List<Event> close() throws IOException {
        super.close();
        bufferManager.freeAllResources();
        return Collections.emptyList();
    }

    @Override
    public void write(Object key, Object value) throws IOException {
        try {
            collect(key, value, partitioner.getPartition(key, value, partitions));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    synchronized void collect(Object key, Object value, final int partition) throws IOException, InterruptedException {
        if (key.getClass() != keyClass) {
            throw new IOException("Type mismatch in key from map: expected "
                    + keyClass.getName() + ", received "
                    + key.getClass().getName());
        }
        if (value.getClass() != valClass) {
            throw new IOException("Type mismatch in value from map: expected "
                    + valClass.getName() + ", received "
                    + value.getClass().getName());
        }
        if (partition < 0 || partition >= partitions) {
            throw new IOException("Illegal partition for " + key + " ("
                    + partition + ")");
        }

        bufferManager.addRecord(partition, key, value);
        numRecordsPerPartition[partition]++;
    }

    public int[] getNumRecordsPerPartition() {
        return numRecordsPerPartition;
    }

    private boolean isMemoryShuffleEnabled(String storageType) {
        return StorageType.withMemory(StorageType.valueOf(storageType));
    }
}