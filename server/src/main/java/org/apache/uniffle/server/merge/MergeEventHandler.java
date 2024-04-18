package org.apache.uniffle.server.merge;

public interface MergeEventHandler {

  void handle(MergeEvent event);

  int getEventNumInMerge();

  void stop();
}