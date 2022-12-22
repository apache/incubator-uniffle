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

package org.apache.uniffle.server.storage.local;

import java.util.ArrayList;
import java.util.List;

import org.apache.uniffle.storage.common.LocalStorage;

/**
 * This class is to wrap multiple local storages into single view.
 */
public class ChainableLocalStorageView {

  private final List<LocalStorage> localStorages;

  public ChainableLocalStorageView(LocalStorage localStorage) {
    this.localStorages = new ArrayList<>();
    localStorages.add(localStorage);
  }

  public synchronized void switchTo(LocalStorage localStorage) {
    localStorages.add(localStorage);
  }

  public synchronized LocalStorage get() {
    return localStorages.get(localStorages.size() - 1);
  }

  public synchronized void remove(LocalStorage localStorage) {
    localStorages.remove(localStorage);
  }

  public LocalStorage get(int index) {
    return localStorages.get(index);
  }
}
