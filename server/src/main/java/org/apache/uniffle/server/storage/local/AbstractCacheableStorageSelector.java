package org.apache.uniffle.server.storage.local;

import org.apache.uniffle.server.event.PurgeEvent;
import org.apache.uniffle.storage.common.Storage;

public abstract class AbstractCacheableStorageSelector implements StorageSelector<Storage>  {

  public abstract void removeCache(PurgeEvent event);

}