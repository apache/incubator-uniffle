package org.apache.tez.runtime.library.common.shuffle.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.common.RssTezConfig;
import org.apache.tez.common.RssTezUtils;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.FetchResult;
import org.apache.tez.runtime.library.common.shuffle.FetchedInputAllocator;
import org.apache.tez.runtime.library.common.shuffle.FetcherCallback;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleReadClient;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.request.CreateShuffleReadClientRequest;
import org.apache.uniffle.common.ShuffleServerInfo;


public class RssTezFetcherTask extends CallableWithNdc<FetchResult> {
  private static final Logger LOG = LoggerFactory.getLogger(RssTezFetcherTask.class);

  private final FetcherCallback fetcherCallback;

  private final InputContext inputContext;
  private Configuration conf;
  private FetchedInputAllocator inputManager;
  private final int partition;  // 本Fetcher要拉取的partition
  //  private InputHost inputHost ;
  List<InputAttemptIdentifier> inputs;
  private Set<ShuffleServerInfo> serverInfoSet;
  Map<Integer, Roaring64NavigableMap> rssAllBlockIdBitmapMap;
  Map<Integer, Roaring64NavigableMap> rssSuccessBlockIdBitmapMap;
  private String clientType = null; // todo： check以下是否可为null
  private final int numPhysicalInputs;
  private final String appId;
  private final int dagIdentifier;
  private final int vertexIndex;
  private final int reduceId;


  private String storageType;
  private String basePath;
  private int indexReadLimit;
  private final int readBufferSize;
  private final int partitionNumPerRange;
  private int partitionNum;

  private JobConf mrJobConf;  // todo, 看下是干嘛的
  private JobConf rssJobConf;
  private final CompressionCodec tezCodec;

  public RssTezFetcherTask(FetcherCallback fetcherCallback, InputContext inputContext, Configuration conf,
                           FetchedInputAllocator inputManager, int partition,
                           List<InputAttemptIdentifier> inputs, Set<ShuffleServerInfo> serverInfoList,
                           Map<Integer, Roaring64NavigableMap> rssAllBlockIdBitmapMap,
                           Map<Integer, Roaring64NavigableMap> rssSuccessBlockIdBitmapMap,
                           int numPhysicalInputs, CompressionCodec tezCodec) {
    assert (inputs != null && inputs.size() > 0);
    this.fetcherCallback = fetcherCallback;
    this.inputContext = inputContext;
    this.conf = conf;
    this.inputManager = inputManager;
    this.partition = partition;  // 要拉取的partition id
    this.inputs = inputs;

    this.serverInfoSet = serverInfoList;
    this.rssAllBlockIdBitmapMap = rssAllBlockIdBitmapMap;
    this.rssSuccessBlockIdBitmapMap = rssSuccessBlockIdBitmapMap;
    this.numPhysicalInputs = numPhysicalInputs;

    this.appId = RssTezUtils.getApplicationAttemptId().toString(); // appattempt_1671091870143_4110_000001 , 与MR 保持一致
    this.dagIdentifier = this.inputContext.getDagIdentifier();
    this.vertexIndex = this.inputContext.getTaskVertexIndex();  // todo： 获取上游Map的vertexId

    this.reduceId =  this.inputContext.getTaskIndex(); // conf.getReduceId();  // 当前这个reduce task的task id
    LOG.info("RssTezFetcherTask, dagIdentifier:{}, vertexIndex:{}, reduceId:{}.", dagIdentifier, vertexIndex, reduceId);
    this.tezCodec = tezCodec;
    this.rssJobConf = new JobConf(RssTezConfig.RSS_CONF_FILE);
    clientType = conf.get(RssTezConfig.RSS_CLIENT_TYPE, RssTezConfig.RSS_CLIENT_TYPE_DEFAULT_VALUE);
    this.storageType = RssTezUtils.getString(conf, conf, RssTezConfig.RSS_STORAGE_TYPE);
    //RssTezUtils.getString(rssJobConf, mrJobConf, RssTezConfig.RSS_STORAGE_TYPE);

    if (storageType == null || storageType.length() <= 1) {
      LOG.warn("storageType is empty");
      this.storageType = "MEMORY_LOCALFILE";
    }
    LOG.info("------------storageType:{}", storageType);

    //    this.basePath
    //    this.indexReadLimit = RssTezUtils.getInt(rssJobConf, mrJobConf, RssTezConfig.RSS_INDEX_READ_LIMIT,
    //        RssTezConfig.RSS_INDEX_READ_LIMIT_DEFAULT_VALUE);
    this.readBufferSize = 1024 * 1024;  // (int) UnitConverter.byteStringAsBytes(
    //    this.readBufferSize = (int) UnitConverter.byteStringAsBytes(
    //        RssTezUtils.getString(rssJobConf, mrJobConf, RssTezConfig.RSS_CLIENT_READ_BUFFER_SIZE,
    //            RssTezConfig.RSS_CLIENT_READ_BUFFER_SIZE_DEFAULT_VALUE));
    this.partitionNumPerRange = 1;// RssTezUtils.getInt(rssJobConf, mrJobConf, RssTezConfig.RSS_PARTITION_NUM_PER_RANGE,
    // RssTezConfig.RSS_PARTITION_NUM_PER_RANGE_DEFAULT_VALUE);
    // this.partitionNum = mrJobConf.getNumReduceTasks(); // todo:  获取下游分区reduce task number
    LOG.info("RssTezFetcherTask is about to fetch partition:{}, with inputs:{}.", this.partition, inputs);
  }


  @Override
  protected FetchResult callInternal() throws Exception {
    // get assigned RSS servers

    // just get blockIds from RSS servers
    // 2. 获取writeClient & blockIdBitmap

    String sourceVertexName = inputContext.getSourceVertexName();
    String taskVertexName = inputContext.getTaskVertexName();
    int shuffleId = RssTezUtils.computeShuffleId(dagIdentifier, inputContext.getSourceVertexName(), taskVertexName);
    ShuffleWriteClient writeClient = RssTezUtils.createShuffleClient(this.conf);
    LOG.info("WriteClient getShuffleResult, clientType:{}, serverInfoSet:{}, appId:{}, shuffleId:{}, partition:{}",
        clientType, serverInfoSet, appId, shuffleId, partition);
    Roaring64NavigableMap blockIdBitmap = writeClient.getShuffleResult(// vertexID 为上游map task的vertexID
        clientType, serverInfoSet, appId, shuffleId, partition);
    writeClient.close();
    LOG.info("RssTezFetcherTask blockIdBitMap:{}", blockIdBitmap);
    rssAllBlockIdBitmapMap.put(partition, blockIdBitmap);
    LOG.info("RssTezFetcherTask rssAllBlockIdBitmapMap:{}", rssAllBlockIdBitmapMap);


    // 3. 获取上游的 taskIdBitmap， todo:
    // get map-completion events to generate RSS taskIDs
    //    final RssEventFetcher eventFetcher = new RssEventFetcher(inputs, numPhysicalInputs);
    int appAttemptId = RssTezUtils.getApplicationAttemptId().getAttemptId();
    Roaring64NavigableMap taskIdBitmap = RssTezUtils.fetchAllRssTaskIds(
        new HashSet<>(inputs), numPhysicalInputs,
        appAttemptId);
    //eventFetcher.parseAllRssTaskIds(); // todo: 获取上游的taskIds
    LOG.info("inputs:{}, numinput:{}, apptatmeid:{},--taskIdBitmap:{}",
        inputs, numPhysicalInputs, appAttemptId, taskIdBitmap);
    // todo 多个顶点，不需要考虑，

    LOG.info("In reduce: " + reduceId
        + ", RSS Tez client has fetched blockIds and taskIds successfully");
    // start fetcher to fetch blocks from RSS servers
    if (!taskIdBitmap.isEmpty()) {
      LOG.info("In reduce: " + reduceId
          + ", Rss Tez client starts to fetch blocks from RSS server");
      JobConf readerJobConf = getRemoteConf();
      LOG.info("------------------------storageType:{}", storageType);
      boolean expectedTaskIdsBitmapFilterEnable = serverInfoSet.size() > 1;
      CreateShuffleReadClientRequest request = new CreateShuffleReadClientRequest(
          appId, RssTezUtils.computeShuffleId(dagIdentifier, sourceVertexName, taskVertexName), partition, storageType,
          basePath, indexReadLimit, readBufferSize,
          partitionNumPerRange, numPhysicalInputs, blockIdBitmap, taskIdBitmap, new ArrayList<>(serverInfoSet),
          readerJobConf, expectedTaskIdsBitmapFilterEnable);
      ShuffleReadClient shuffleReadClient = ShuffleClientFactory.getInstance().createShuffleReadClient(request);
      RssTezFetcher fetcher = new RssTezFetcher(fetcherCallback, this.conf,
          inputManager,
          shuffleReadClient,
          rssSuccessBlockIdBitmapMap,
          partition,
          blockIdBitmap.getLongCardinality(), tezCodec, RssTezConfig.toRssConf(this.conf),
          serverInfoSet); // todo new RssFetcher(new JobConf(this.conf), reduceId,
      // todo: taskStatus, merger, copyPhase, reporter, metrics,
      // todo  shuffleReadClient, blockIdBitmap.getLongCardinality(),
      // todo: RssTezConfig.toRssConf(this.conf));
      fetcher.fetchAllRssBlocks();   // todo: 注释，防止编译错误
      LOG.info("In reduce: " + partition
          + ", Rss Tez client fetches blocks from RSS server successfully");
      // 多个的合并到一起
      // 数据已经拉取完毕，待读取
    }
    // todo 映射关系

    return null;
  }


  public void shutdown() {
    // todo，从Fetcher copy过来，为可编译通过
  }

  private JobConf getRemoteConf() {
    JobConf readerJobConf = new JobConf(conf);
    // todo: P1: 暂时注释，看下这块代码时干嘛的
    return readerJobConf;
  }

  public int getPartitionId() {
    return partition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RssTezFetcherTask that = (RssTezFetcherTask) o;
    return partition == that.partition
        && numPhysicalInputs == that.numPhysicalInputs
        && dagIdentifier == that.dagIdentifier
        && vertexIndex == that.vertexIndex
        && reduceId == that.reduceId
        && Objects.equals(appId, that.appId);

  }

  @Override
  public int hashCode() {
    return Objects.hash(partition, numPhysicalInputs, dagIdentifier, vertexIndex, reduceId, appId);
  }
}
