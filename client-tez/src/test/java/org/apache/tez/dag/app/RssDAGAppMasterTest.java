/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.app;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.common.AsyncDispatcher;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.security.ACLManager;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeManagerPluginContext;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.impl.DAGImpl;
import org.apache.tez.dag.history.HistoryEventHandler;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.hadoop.shim.DefaultHadoopShim;
import org.apache.tez.hadoop.shim.HadoopShim;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfig;
import org.apache.tez.runtime.library.conf.UnorderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.input.RssOrderedGroupedKVInput;
import org.apache.tez.runtime.library.input.RssUnorderedKVInput;
import org.apache.tez.runtime.library.output.RssOrderedPartitionedKVOutput;
import org.apache.tez.runtime.library.output.RssUnorderedKVOutput;
import org.apache.tez.runtime.library.output.RssUnorderedPartitionedKVOutput;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.processor.SimpleProcessor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.storage.util.StorageType;

import static org.apache.tez.common.RssTezConfig.RSS_AM_SHUFFLE_MANAGER_ADDRESS;
import static org.apache.tez.common.RssTezConfig.RSS_AM_SHUFFLE_MANAGER_PORT;
import static org.apache.tez.common.RssTezConfig.RSS_SHUFFLE_DESTINATION_VERTEX_ID;
import static org.apache.tez.common.RssTezConfig.RSS_SHUFFLE_SOURCE_VERTEX_ID;
import static org.apache.tez.common.RssTezConfig.RSS_STORAGE_TYPE;
import static org.apache.tez.runtime.library.api.TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RssDAGAppMasterTest {

  @Test
  public void testHookAfterDagInited() throws Exception {
    // 1 Init and mock some basic module
    AppContext appContext = mock(AppContext.class);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1);
    HadoopShim defaultShim = new DefaultHadoopShim();
    when(appContext.getHadoopShim()).thenReturn(defaultShim);
    when(appContext.getApplicationID()).thenReturn(appAttemptId.getApplicationId());
    ClusterInfo clusterInfo = new ClusterInfo();
    clusterInfo.setMaxContainerCapability(Resource.newInstance(Integer.MAX_VALUE, Integer.MAX_VALUE));
    when(appContext.getClusterInfo()).thenReturn(clusterInfo);
    HistoryEventHandler historyEventHandler = mock(HistoryEventHandler.class);
    doReturn(historyEventHandler).when(appContext).getHistoryHandler();
    ACLManager aclManager = new ACLManager("amUser");
    doReturn(aclManager).when(appContext).getAMACLManager();
    RssDAGAppMaster appMaster = mock(RssDAGAppMaster.class);
    TezRemoteShuffleManager shuffleManager = mock(TezRemoteShuffleManager.class);
    InetSocketAddress address = NetUtils.createSocketAddrForHost("host", 0);
    when(shuffleManager.getAddress()).thenReturn(address);
    when(appMaster.getTezRemoteShuffleManager()).thenReturn(shuffleManager);
    Configuration clientConf = new Configuration(false);
    clientConf.set(RSS_STORAGE_TYPE, StorageType.MEMORY_LOCALFILE_HDFS.name());
    clientConf.set("tez.config1", "value1");
    clientConf.set("config2", "value2");
    Map<String, String> dynamicConf = new HashMap();
    dynamicConf.put(RSS_STORAGE_TYPE, StorageType.LOCALFILE.name());
    dynamicConf.put("tez.config3", "value3");
    when(appMaster.getClusterClientConf()).thenReturn(dynamicConf);
    when(appMaster.getConfig()).thenReturn(clientConf);

    // 2 init dispatcher
    AsyncDispatcher dispatcher = new AsyncDispatcher("core");

    // 3 init dag
    Configuration conf = new Configuration();
    DAG dag = createDAG("test", conf);
    TezDAGID dagId = TezDAGID.getInstance(appAttemptId.getApplicationId(), 1);
    DAGProtos.DAGPlan dagPlan = dag.createDag(conf, null, null, null, false, null, null);
    DAGImpl dagImpl = new DAGImpl(dagId, conf, dagPlan, dispatcher.getEventHandler(), null, new Credentials(),
        new SystemClock(), "user", null, appContext);
    when(appContext.getCurrentDAG()).thenReturn(dagImpl);

    // 4 register call back function
    RssDAGAppMaster.registerStateEnteredCallback(dagImpl, appMaster);

    // 5 register DAGEvent, init and start dispatcher
    EventHandler<DAGEvent> dagEventDispatcher = new EventHandler<DAGEvent>() {
      @Override
      public void handle(DAGEvent event) {
        dagImpl.handle(event);
      }
    };
    dispatcher.register(DAGEventType.class, dagEventDispatcher);
    dispatcher.init(conf);
    dispatcher.start();

    // 6 send DAG_INIT to dispatcher
    dispatcher.getEventHandler().handle(new DAGEvent(dagImpl.getID(), DAGEventType.DAG_INIT));

    // 7 wait DAGImpl transient to INITED state
    await().atMost(2, TimeUnit.SECONDS).until(() -> dagImpl.getState().equals(DAGState.INITED));

    // 8 verify I/O for vertexImpl
    verfiyOutput(dagImpl, "vertex1", RssOrderedPartitionedKVOutput.class.getName(), 0, 1);
    verfiyInput(dagImpl, "vertex2", RssOrderedGroupedKVInput.class.getName(), 0, 1);
    verfiyOutput(dagImpl, "vertex2", RssUnorderedKVOutput.class.getName(), 1, 2);
    verfiyInput(dagImpl, "vertex3", RssUnorderedKVInput.class.getName(), 1, 2);
    verfiyOutput(dagImpl, "vertex3", RssUnorderedPartitionedKVOutput.class.getName(), 2, 3);
    verfiyInput(dagImpl, "vertex4", RssUnorderedKVInput.class.getName(), 2, 3);
  }

  public static void verfiyInput(DAGImpl dag, String name, String expectedInputClassName,
                                 int expectedSourceVertexId, int expectedDestinationVertexId) throws Exception {
    // 1 verfiy rename rss io class name
    List<InputSpec> inputSpecs = dag.getVertex(name).getInputSpecList(0);
    Assertions.assertEquals(1, inputSpecs.size());
    Assertions.assertEquals(expectedInputClassName, inputSpecs.get(0).getInputDescriptor().getClassName());
    // 2 verfiy the address and port of shuffle manager 
    UserPayload payload = inputSpecs.get(0).getInputDescriptor().getUserPayload();
    Configuration conf = TezUtils.createConfFromUserPayload(payload);
    Assertions.assertEquals("host", conf.get(RSS_AM_SHUFFLE_MANAGER_ADDRESS));
    Assertions.assertEquals(0, conf.getInt(RSS_AM_SHUFFLE_MANAGER_PORT, -1));
    // 3 verfiy the config
    Assertions.assertEquals(StorageType.LOCALFILE.name(), conf.get(RSS_STORAGE_TYPE));
    Assertions.assertEquals("value1", conf.get("tez.config1"));
    Assertions.assertEquals("value3", conf.get("tez.config3"));
    Assertions.assertNull(conf.get("tez.config2"));
    // TEZ_RUNTIME_IFILE_READAHEAD_BYTES is in getConfigurationKeySet, so the config from client should deliver
    // to Input/Output. But tez.config.from.client is not in getConfigurationKeySet, so the config from client
    // should not deliver to Input/Output.
    Assertions.assertEquals(12345, conf.getInt(TEZ_RUNTIME_IFILE_READAHEAD_BYTES, -1));
    Assertions.assertNull(conf.get("tez.config.from.client"));
    // 4 verfiy vertex id
    Assertions.assertEquals(expectedSourceVertexId, conf.getInt(RSS_SHUFFLE_SOURCE_VERTEX_ID, -1));
    Assertions.assertEquals(expectedDestinationVertexId, conf.getInt(RSS_SHUFFLE_DESTINATION_VERTEX_ID, -1));
  }

  public static void verfiyOutput(DAGImpl dag, String name, String expectedOutputClassName,
                                  int expectedSourceVertexId, int expectedDestinationVertexId) throws Exception {
    // 1 verfiy rename rss io class name
    List<OutputSpec> outputSpecs = dag.getVertex(name).getOutputSpecList(0);
    Assertions.assertEquals(1, outputSpecs.size());
    Assertions.assertEquals(expectedOutputClassName, outputSpecs.get(0).getOutputDescriptor().getClassName());
    // 2 verfiy the address and port of shuffle manager
    UserPayload payload = outputSpecs.get(0).getOutputDescriptor().getUserPayload();
    Configuration conf = TezUtils.createConfFromUserPayload(payload);
    Assertions.assertEquals("host", conf.get(RSS_AM_SHUFFLE_MANAGER_ADDRESS));
    Assertions.assertEquals(0, conf.getInt(RSS_AM_SHUFFLE_MANAGER_PORT, -1));
    // 3 verfiy the config
    Assertions.assertEquals(StorageType.LOCALFILE.name(), conf.get(RSS_STORAGE_TYPE));
    Assertions.assertEquals("value1", conf.get("tez.config1"));
    Assertions.assertEquals("value3", conf.get("tez.config3"));
    Assertions.assertNull(conf.get("tez.config2"));
    // 4 verfiy vertex id
    Assertions.assertEquals(expectedSourceVertexId, conf.getInt(RSS_SHUFFLE_SOURCE_VERTEX_ID, -1));
    Assertions.assertEquals(expectedDestinationVertexId, conf.getInt(RSS_SHUFFLE_DESTINATION_VERTEX_ID, -1));
  }

  private static DAG createDAG(String dageName, Configuration conf) {
    conf.setInt(TEZ_RUNTIME_IFILE_READAHEAD_BYTES, 12345);
    conf.set("tez.config.from.client", "value.from.client");

    DataSourceDescriptor dummyInput = DataSourceDescriptor.create(
        InputDescriptor.create("dummyclass"), InputInitializerDescriptor.create(""), null);

    EdgeManagerPluginDescriptor cpEdgeManager =
        EdgeManagerPluginDescriptor.create(DummyProductEdgeManager.class.getName());

    Vertex vertex1 = Vertex.create("vertex1", ProcessorDescriptor.create(DummyOp.class.getName()));
    Vertex vertex2 = Vertex.create("vertex2", ProcessorDescriptor.create(DummyOp.class.getName()));
    Vertex vertex3 = Vertex.create("vertex3", ProcessorDescriptor.create(DummyOp.class.getName()));
    Vertex vertex4 = Vertex.create("vertex4", ProcessorDescriptor.create(DummyOp.class.getName()));

    vertex1.addDataSource("dummyInput", dummyInput);
    OrderedPartitionedKVEdgeConfig edgeConf12 =
        OrderedPartitionedKVEdgeConfig.newBuilder(NullWritable.class.getName(), NullWritable.class.getName(),
            HashPartitioner.class.getName()).setFromConfiguration(conf).build();
    UnorderedKVEdgeConfig edgeConf23 =
        UnorderedKVEdgeConfig.newBuilder(NullWritable.class.getName(), NullWritable.class.getName())
            .setFromConfiguration(conf).build();
    UnorderedPartitionedKVEdgeConfig edgeConf34 =
        UnorderedPartitionedKVEdgeConfig.newBuilder(NullWritable.class.getName(), NullWritable.class.getName(),
            HashPartitioner.class.getName()).setFromConfiguration(conf).build();

    DAG dag = DAG.create(dageName);
    dag.addVertex(vertex1)
        .addVertex(vertex2)
        .addVertex(vertex3)
        .addVertex(vertex4)
        .addEdge(Edge.create(vertex1, vertex2, edgeConf12.createDefaultCustomEdgeProperty(cpEdgeManager)))
        .addEdge(Edge.create(vertex2, vertex3, edgeConf23.createDefaultCustomEdgeProperty(cpEdgeManager)))
        .addEdge(Edge.create(vertex3, vertex4, edgeConf34.createDefaultCustomEdgeProperty(cpEdgeManager)));
    return dag;
  }

  public static class DummyOp extends SimpleProcessor {

    public DummyOp(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() {
    }
  }

  public static class DummyProductEdgeManager extends EdgeManagerPluginOnDemand {

    public DummyProductEdgeManager(EdgeManagerPluginContext context) {
      super(context);
    }

    @Override
    public void initialize() throws Exception {
    }

    @Override
    public void prepareForRouting() throws Exception {
    }

    @Override
    public int getNumDestinationTaskPhysicalInputs(int destinationTaskIndex) throws Exception {
      return 1;
    }

    @Override
    public int getNumSourceTaskPhysicalOutputs(int sourceTaskIndex) throws Exception {
      return 1;
    }

    @Override
    public int getNumDestinationConsumerTasks(int sourceTaskIndex) throws Exception {
      return 1;
    }

    @Override
    public int routeInputErrorEventToSource(int destinationTaskIndex, int destinationFailedInputIndex)
        throws Exception {
      return 1;
    }

    @Nullable
    @Override
    public EventRouteMetadata routeDataMovementEventToDestination(int sourceTaskIndex, int sourceOutputIndex,
        int destinationTaskIndex) throws Exception {
      return null;
    }

    @Nullable
    @Override
    public CompositeEventRouteMetadata routeCompositeDataMovementEventToDestination(int sourceTaskIndex,
        int destinationTaskIndex) throws Exception {
      return null;
    }

    @Nullable
    @Override
    public EventRouteMetadata routeInputSourceTaskFailedEventToDestination(int sourceTaskIndex, 
        int destinationTaskIndex) throws Exception {
      return null;
    }
  }
}
