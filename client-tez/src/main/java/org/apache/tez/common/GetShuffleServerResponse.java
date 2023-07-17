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

package org.apache.tez.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.Writable;

public class GetShuffleServerResponse implements Writable {
  private int status;
  private String retMsg;
  private ShuffleAssignmentsInfoWritable shuffleAssignmentsInfoWritable;

  public int getStatus() {
    return status;
  }

  public void setStatus(int status) {
    this.status = status;
  }

  public String getRetMsg() {
    return retMsg;
  }

  public void setRetMsg(String retMsg) {
    this.retMsg = retMsg;
  }

  public ShuffleAssignmentsInfoWritable getShuffleAssignmentsInfoWritable() {
    return shuffleAssignmentsInfoWritable;
  }

  public void setShuffleAssignmentsInfoWritable(
      ShuffleAssignmentsInfoWritable shuffleAssignmentsInfoWritable) {
    this.shuffleAssignmentsInfoWritable = shuffleAssignmentsInfoWritable;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(status);
    dataOutput.writeUTF(retMsg);
    if (Objects.isNull(shuffleAssignmentsInfoWritable)) {
      dataOutput.writeInt(-1);
    } else {
      dataOutput.writeInt(1);
      shuffleAssignmentsInfoWritable.write(dataOutput);
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    status = dataInput.readInt();
    retMsg = dataInput.readUTF();
    shuffleAssignmentsInfoWritable = new ShuffleAssignmentsInfoWritable();
    if (dataInput.readInt() != -1) {
      shuffleAssignmentsInfoWritable.readFields(dataInput);
    }
  }
}
