/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available. 
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.spark.shuffle.writer;

import com.tencent.rss.common.ShuffleBlockInfo;
import java.util.List;

public class AddBlockEvent {

  private String taskId;
  private List<ShuffleBlockInfo> shuffleDataInfoList;

  public AddBlockEvent(String taskId, List<ShuffleBlockInfo> shuffleDataInfoList) {
    this.taskId = taskId;
    this.shuffleDataInfoList = shuffleDataInfoList;
  }

  public String getTaskId() {
    return taskId;
  }

  public List<ShuffleBlockInfo> getShuffleDataInfoList() {
    return shuffleDataInfoList;
  }

  @Override
  public String toString() {
    return "AddBlockEvent: TaskId[" + taskId + "], " + shuffleDataInfoList;
  }
}
