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

import java.util.ArrayList;
import java.util.List;

import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.records.DAGProtos;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RssDAGAppMasterTest {

  @Test
  public void testAddAdditionalResource() throws TezException {
    DAGProtos.DAGPlan dagPlan = DAGProtos.DAGPlan.getDefaultInstance();
    List<DAGProtos.PlanLocalResource> originalResources = dagPlan.getLocalResourceList();
    if (originalResources == null) {
      originalResources = new ArrayList<>();
    } else {
      originalResources = new ArrayList<>(originalResources);
    }

    DAGProtos.PlanLocalResource additionalResource = DAGProtos.PlanLocalResource.newBuilder()
            .setName("rss_conf.xml")
            .setUri("/data1/test")
            .setSize(12)
            .setTimeStamp(System.currentTimeMillis())
            .setType(DAGProtos.PlanLocalResourceType.FILE)
            .setVisibility(DAGProtos.PlanLocalResourceVisibility.APPLICATION)
            .build();

    RssDAGAppMaster.addAdditionalResource(dagPlan, additionalResource);
    List<DAGProtos.PlanLocalResource> newResources = dagPlan.getLocalResourceList();

    originalResources.add(additionalResource);

    assertEquals(originalResources.size(), newResources.size());
    for (int i = 0; i < originalResources.size(); i++) {
      assertEquals(originalResources.get(i), newResources.get(i));
    }
  }
}
