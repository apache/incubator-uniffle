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

package org.apache.uniffle.client.response;

import org.apache.uniffle.common.rpc.StatusCode;

public class RssReassignServersReponse extends ClientResponse {

  private boolean needReassign;

  public RssReassignServersReponse(StatusCode statusCode, String message, boolean needReassign) {
    super(statusCode, message);
    this.needReassign = needReassign;
  }

  public boolean isNeedReassign() {
    return needReassign;
  }

  public static RssReassignServersReponse fromProto(RssProtos.ReassignServersReponse response) {
    return new RssReassignServersReponse(
        // todo: [issue#780] add fromProto for StatusCode issue
        StatusCode.valueOf(response.getStatus().name()),
        response.getMsg(),
        response.getNeedReassign());
  }
}
