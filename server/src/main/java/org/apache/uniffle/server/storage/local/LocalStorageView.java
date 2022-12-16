package org.apache.uniffle.server.storage.local;

import java.util.ArrayList;
import java.util.List;

import org.apache.uniffle.storage.common.LocalStorage;

public class LocalStorageView {

  private final List<LocalStorage> localStorages;

  public LocalStorageView(LocalStorage localStorage) {
    this.localStorages = new ArrayList<>();
    localStorages.add(localStorage);
  }

  public void add(LocalStorage localStorage) {
    localStorages.add(localStorage);
  }

  public LocalStorage getLatest() {
    return localStorages.get(localStorages.size() - 1);
  }

  public void removeTail() {
    localStorages.remove(localStorages.size() - 1);
  }

  public LocalStorage get(int index) {
    return localStorages.get(index);
  }
}