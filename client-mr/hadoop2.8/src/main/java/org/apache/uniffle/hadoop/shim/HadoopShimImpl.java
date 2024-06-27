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

package org.apache.uniffle.hadoop.shim;

import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.reduce.ShuffleClientMetrics;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerAllocator;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopShimImpl {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopShimImpl.class);

  public static ShuffleClientMetrics createShuffleClientMetrics(
      TaskAttemptID taskAttemptID, JobConf jobConf) {
    try {
      Constructor constructor =
          ShuffleClientMetrics.class.getDeclaredConstructor(
              new Class[] {TaskAttemptID.class, JobConf.class});
      constructor.setAccessible(true);
      return (ShuffleClientMetrics) constructor.newInstance(taskAttemptID, jobConf);
    } catch (Exception e) {
      LOG.warn("Construct ShuffleClientMetrics fail, caused by {}", e);
      return null;
    }
  }

  public static RMContainerAllocator createRMContainerAllocator(
      ClientService clientService, AppContext context) {
    return new RMContainerAllocator(clientService, context) {
      @Override
      protected AllocateResponse makeRemoteRequest() throws YarnException, IOException {
        AllocateResponse response = super.makeRemoteRequest();
        // UpdateNodes only have one use for MRAppMaster, MRAppMaster use the updateNodes to find
        // which
        // nodes are bad nodes. So we clear them, MRAppMaster will not recompute the map tasks.
        response.getUpdatedNodes().clear();
        return response;
      }
    };
  }
}
