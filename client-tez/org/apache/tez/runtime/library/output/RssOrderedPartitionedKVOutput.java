package org.apache.tez.runtime.library.output;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.Deflater;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.GetShuffleServerRequest;
import org.apache.tez.common.GetShuffleServerResponse;
import org.apache.tez.common.RssTezConfig;
import org.apache.tez.common.RssTezUtils;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezRemoteShuffleUmbilicalProtocol;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.AbstractLogicalOutput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.Writer;
import org.apache.tez.runtime.library.api.KeyValuesWriter;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.sort.impl.ExternalSorter;
import org.apache.tez.runtime.library.common.sort.impl.RssSorter;
import org.apache.tez.runtime.library.common.sort.impl.RssTezSpillRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShuffleServerInfo;

import static org.apache.tez.common.RssTezUtils.HOST_NAME;
import static org.apache.tez.common.RssTezUtils.uniformPartitionHostInfo;


/**
 * {@link RssOrderedPartitionedKVOutput} is an {@link AbstractLogicalOutput} which
 * support remote shuffle.
 *
 */
@Public
public class RssOrderedPartitionedKVOutput extends AbstractLogicalOutput {
    private static final Logger LOG = LoggerFactory.getLogger(RssOrderedPartitionedKVOutput.class);
    protected ExternalSorter sorter;
    protected Configuration conf;
    protected MemoryUpdateCallbackHandler memoryUpdateCallbackHandler;
    private long startTime;
    private long endTime;
    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private final Deflater deflater;
    private Map<Integer, List<ShuffleServerInfo>> partitionToServers;
    private int mapNum;
    private int numOutputs;
    private TezTaskAttemptID taskAttemptId;
    private ApplicationId applicationId;
    private boolean sendEmptyPartitionDetails;
    private OutputContext outputContext;
    private String host;
    private int port;
    private String taskVertexName;
    private String destinationVertexName;
    private int shuffleId;


    public RssOrderedPartitionedKVOutput(OutputContext outputContext, int numPhysicalOutputs) {
        super(outputContext, numPhysicalOutputs);
        this.outputContext = outputContext;
        this.deflater = TezCommonUtils.newBestCompressionDeflater();
        this.numOutputs = getNumPhysicalOutputs();
        this.mapNum = outputContext.getVertexParallelism();
        this.applicationId = outputContext.getApplicationId();
        this.taskAttemptId = TezTaskAttemptID.fromString(
                RssTezUtils.uniqueIdentifierToAttemptId(outputContext.getUniqueIdentifier()));
        this.taskVertexName = outputContext.getTaskVertexName();
        this.destinationVertexName = outputContext.getDestinationVertexName();
        LOG.info("taskAttemptId is {}", taskAttemptId.toString());
        LOG.info("taskVertexName is {}", taskVertexName);
        LOG.info("destinationVertexName is {}", destinationVertexName);
        LOG.info("Initialized RssOrderedPartitionedKVOutput.");
    }

    private void getRssConf() {
        try {
            JobConf conf = new JobConf(RssTezConfig.RSS_CONF_FILE);
            this.host = conf.get(RssTezConfig.RSS_AM_SHUFFLE_MANAGER_ADDRESS, "null host");
            this.port = conf.getInt(RssTezConfig.RSS_AM_SHUFFLE_MANAGER_PORT, -1);

            LOG.info("Got RssConf am info : host is {}, port is {}", host, port);
        } catch (Exception e) {
            LOG.warn("debugRssConf error: ", e);
        }
    }

    @Override
    public List<Event> initialize() throws Exception {
        this.startTime = System.nanoTime();
        this.conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
        this.memoryUpdateCallbackHandler = new MemoryUpdateCallbackHandler();

        long memRequestSize = RssTezUtils.getInitialMemoryRequirement(conf, getContext().getTotalMemoryAvailableToTask());
        LOG.info("memRequestSize is {}", memRequestSize);
        getContext().requestInitialMemory(memRequestSize, memoryUpdateCallbackHandler);
        LOG.info("Got initialMemory.");

        getRssConf();

        this.sendEmptyPartitionDetails = conf.getBoolean(
                TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED,
                TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED_DEFAULT);

        final InetSocketAddress address = NetUtils.createSocketAddrForHost(host, port);

        UserGroupInformation taskOwner = UserGroupInformation.createRemoteUser(this.applicationId.toString());

        TezVertexID tezVertexID = taskAttemptId.getTaskID().getVertexID();
        TezDAGID tezDAGID = tezVertexID.getDAGId();
        this.shuffleId = RssTezUtils.computeShuffleId(tezDAGID.getId(), this.taskVertexName, this.destinationVertexName);
        LOG.info("Generate shuffleId is {}", shuffleId);

        TezRemoteShuffleUmbilicalProtocol umbilical = taskOwner
                .doAs(new PrivilegedExceptionAction<TezRemoteShuffleUmbilicalProtocol>() {
                    @Override
                    public TezRemoteShuffleUmbilicalProtocol run() throws Exception {
                        return RPC.getProxy(TezRemoteShuffleUmbilicalProtocol.class,
                                TezRemoteShuffleUmbilicalProtocol.versionID,
                                address, conf);
                    }
                });

        GetShuffleServerRequest request = new GetShuffleServerRequest(this.taskAttemptId, this.mapNum,
                this.numOutputs, this.shuffleId);
        LOG.info("RequestInfo taskAttemptId is {}, mapNum is {}, numOutputs is {}",
                taskAttemptId.toString(), mapNum, numOutputs);
        GetShuffleServerResponse response = umbilical.getShuffleAssignments(request);
        this.partitionToServers = response.getShuffleAssignmentsInfoWritable()
                .getShuffleAssignmentsInfo()
                .getPartitionToServers();

        LOG.info("Got response from am.");

        LOG.info("Got the original host : {}", this.outputContext.getExecutionContext().getHostName());

        String uniformPartitionHostInfo = uniformPartitionHostInfo(partitionToServers);

        modifyHost(this.destinationVertexName, uniformPartitionHostInfo);

        LOG.info("Set the rss worker host : {}", uniformPartitionHostInfo);

        return Collections.emptyList();
    }

    public String modifyHost(String vertexName, String uniformPartitionHostInfo)
            throws NoSuchFieldException, IllegalAccessException {
        synchronized (outputContext.getExecutionContext()) {
            LOG.info("Got host info is {}", uniformPartitionHostInfo);
            assert uniformPartitionHostInfo != null;
            Field field = outputContext.getExecutionContext().getClass().getDeclaredField(HOST_NAME);
            String lastHostName = outputContext.getExecutionContext().getHostName();
            String appendHostName = vertexName + "=" + uniformPartitionHostInfo;
            String modifyHostName = lastHostName + ";" + appendHostName;
            LOG.info("lastHostName is {}, appendHostName is {}, modifyHostName is {}",
                    lastHostName, appendHostName, modifyHostName);
            field.setAccessible(true);
            field.set(this.outputContext.getExecutionContext(), modifyHostName);
            field.setAccessible(false);
            return outputContext.getExecutionContext().getHostName();
        }
    }

    @Override
    public void handleEvents(List<Event> list) {
        // Not expecting any events.
    }

    @Override
    public List<Event> close() throws Exception {
        List<Event> returnEvents = Lists.newLinkedList();
        if (sorter != null) {
            sorter.flush();
            returnEvents.addAll(sorter.close());
            this.endTime = System.nanoTime();
            returnEvents.addAll(generateEvents());
            sorter = null;
        } else {
            LOG.warn(getContext().getDestinationVertexName()
                            + ": Attempting to close output {} of type {} before it was started. Generating empty events",
                    getContext().getDestinationVertexName(), this.getClass().getSimpleName());
            returnEvents = generateEmptyEvents();
        }
        LOG.info("RssOrderedPartitionedKVOutput close.");
        return returnEvents;
    }

    @Override
    public void start() throws Exception {
        if (!isStarted.get()) {
            memoryUpdateCallbackHandler.validateUpdateReceived();
            sorter = new RssSorter(taskAttemptId, getContext(), conf, mapNum, numOutputs,
                    memoryUpdateCallbackHandler.getMemoryAssigned(), taskAttemptId, shuffleId,
                    partitionToServers);
            LOG.info("Initialized RssSorter.");
            isStarted.set(true);
        }
    }

    @Override
    public Writer getWriter() throws IOException {
        Preconditions.checkState(isStarted.get(), "Cannot get writer before starting the Output");

        return new KeyValuesWriter() {
            @Override
            public void write(Object key, Iterable<Object> values) throws IOException {
                sorter.write(key, values);
            }

            @Override
            public void write(Object key, Object value) throws IOException {
                sorter.write(key, value);
            }
        };
    }

    private List<Event> generateEvents() throws IOException {
        List<Event> eventList = Lists.newLinkedList();
        boolean isLastEvent = true;

        String auxiliaryService = conf.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
                TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT);

        int[] numRecordsPerPartition = ((RssSorter) sorter).getNumRecordsPerPartition();
        RssTezSpillRecord rssTezSpillRecord = new RssTezSpillRecord(numOutputs, numRecordsPerPartition);

        LOG.info("RssTezSpillRecord is initialized");

        ShuffleUtils.generateEventOnSpill(eventList, true, isLastEvent,
                getContext(), 0, rssTezSpillRecord,
                getNumPhysicalOutputs(), sendEmptyPartitionDetails, getContext().getUniqueIdentifier(),
                sorter.getPartitionStats(), sorter.reportDetailedPartitionStats(), auxiliaryService, deflater);
        LOG.info("Generate events.");

        return eventList;
    }

    private List<Event> generateEmptyEvents() throws IOException {
        List<Event> eventList = Lists.newArrayList();
        ShuffleUtils.generateEventsForNonStartedOutput(eventList,
                getNumPhysicalOutputs(),
                getContext(),
                true,
                true,
                deflater);
        LOG.info("Generate empty events.");
        return eventList;
    }

    private static final Set<String> confKeys = new HashSet<String>();

    static {
        confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD);
        confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES);
        confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IO_FILE_BUFFER_SIZE);
        confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES);
        confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB);
        confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_MAX_PER_BUFFER_SIZE_BYTES);
        confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS);
        confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS);
        confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS);
        confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS);
        confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC);
        confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED);
        confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_CONVERT_USER_PAYLOAD_TO_HISTORY_TEXT);
        confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED);
        confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT);
        confKeys.add(TezConfiguration.TEZ_COUNTERS_MAX);
        confKeys.add(TezConfiguration.TEZ_COUNTERS_GROUP_NAME_MAX_LENGTH);
        confKeys.add(TezConfiguration.TEZ_COUNTERS_COUNTER_NAME_MAX_LENGTH);
        confKeys.add(TezConfiguration.TEZ_COUNTERS_MAX_GROUPS);
        confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_CLEANUP_FILES_ON_INTERRUPT);
        confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS);
        confKeys.add(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID);
        confKeys.add(
                TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_PARTITIONED_KVWRITER_BUFFER_MERGE_PERCENT);
    }

    @InterfaceAudience.Private
    public static Set<String> getConfigurationKeySet() {
        return Collections.unmodifiableSet(confKeys);
    }

}