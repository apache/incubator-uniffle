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
import org.apache.uniffle.proto.RssProtos.ReportShuffleWriteFailureResponse;

public class RssReportShuffleWriteFailureResponse extends ClientResponse {
  private boolean reSubmitWholeStage;

  public RssReportShuffleWriteFailureResponse(StatusCode code, String msg, boolean recomputeStage) {
    super(code, msg);
    this.reSubmitWholeStage = recomputeStage;
  }

  public boolean getReSubmitWholeStage() {
    return this.reSubmitWholeStage;
  }

  public ReportShuffleWriteFailureResponse toProto() {
    ReportShuffleWriteFailureResponse.Builder builder =
        ReportShuffleWriteFailureResponse.newBuilder();
    return builder
        .setStatus(getStatusCode().toProto())
        .setMsg(getMessage())
        .setReSubmitWholeStage(reSubmitWholeStage)
        .build();
  }

  public static RssReportShuffleWriteFailureResponse fromProto(
      ReportShuffleWriteFailureResponse response) {
    return new RssReportShuffleWriteFailureResponse(
        // todo: [issue#780] add fromProto for StatusCode issue
        StatusCode.valueOf(response.getStatus().name()),
        response.getMsg(),
        response.getReSubmitWholeStage());
  }
}
