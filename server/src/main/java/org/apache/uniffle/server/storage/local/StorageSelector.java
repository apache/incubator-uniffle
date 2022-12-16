package org.apache.uniffle.server.storage.local;

import org.apache.uniffle.server.ShuffleDataFlushEvent;
import org.apache.uniffle.server.ShuffleDataReadEvent;
import org.apache.uniffle.storage.common.Storage;

public interface StorageSelector<T extends Storage> {

  T selectForWriter(ShuffleDataFlushEvent event);

  T getForReader(ShuffleDataReadEvent event);
}