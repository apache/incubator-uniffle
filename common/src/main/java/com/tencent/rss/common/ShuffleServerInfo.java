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

package com.tencent.rss.common;

import java.io.Serializable;

public class ShuffleServerInfo implements Serializable {

  private String id;

  private String host;

  private int port;

  public ShuffleServerInfo(String id, String host, int port) {
    this.id = id;
    this.host = host;
    this.port = port;
  }

  public String getId() {
    return id;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  @Override
  public int hashCode() {
    return host.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ShuffleServerInfo) {
      return id.equals(((ShuffleServerInfo) obj).getId())
          && host.equals(((ShuffleServerInfo) obj).getHost())
          && port == ((ShuffleServerInfo) obj).getPort();
    }
    return false;
  }

  @Override
  public String toString() {
    return "ShuffleServerInfo{id[" + id + "], host[" + host + "], port[" + port + "]}";
  }
}
