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

package org.apache.uniffle.flink.reader.virtual;

import java.net.InetAddress;

import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.SubpartitionIndexRange;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.LocalConnectionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.throughput.ThroughputCalculator;
import org.apache.flink.util.clock.SystemClock;

public class FakedRssInputChannel extends RemoteInputChannel {
  public FakedRssInputChannel(int gateIndex, int channelIndex) {

    super(
        new SingleInputGate(
            "",
            gateIndex,
            new IntermediateDataSetID(),
            ResultPartitionType.BLOCKING,
            new SubpartitionIndexRange(0, 0),
            1,
            (a, b, c) -> {},
            () -> null,
            null,
            new FakedRssMemorySegmentProvider(),
            0,
            new ThroughputCalculator(SystemClock.getInstance()),
            null),
        channelIndex,
        new ResultPartitionID(),
        0,
        new ConnectionID(
            new TaskManagerLocation(ResourceID.generate(), InetAddress.getLoopbackAddress(), 1), 0),
        new LocalConnectionManager(),
        0,
        0,
        0,
        new SimpleCounter(),
        new SimpleCounter(),
        new FakedRssChannelStateWriter());
  }
}
