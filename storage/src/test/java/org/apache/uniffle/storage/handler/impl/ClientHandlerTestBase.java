package org.apache.uniffle.storage.handler.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.ShuffleDataSegment;
import org.apache.uniffle.common.ShuffleIndexResult;
import org.apache.uniffle.common.segment.SegmentSplitter;

public class ClientHandlerTestBase {
  public static class FakeSegmentSplitter implements SegmentSplitter {
    private List<ShuffleDataSegment> shuffleDataSegments;

    public FakeSegmentSplitter(List<ShuffleDataSegment> shuffleDataSegments) {
      this.shuffleDataSegments = shuffleDataSegments;
    }

    @Override
    public List<ShuffleDataSegment> split(ShuffleIndexResult shuffleIndexResult) {
      return shuffleDataSegments;
    }
  }

  public static class FakeHandler extends DataSkippableReadHandler {
    public List<Long> blockIds = new CopyOnWriteArrayList<>();

    public FakeHandler(Roaring64NavigableMap expectBlockIds, Roaring64NavigableMap processBlockIds,
        SegmentSplitter segmentSplitter) {
      super("appId", 1, 1, 64, expectBlockIds, processBlockIds, segmentSplitter);
    }

    @Override
    protected ShuffleIndexResult readShuffleIndex() {
      return new ShuffleIndexResult(new byte[100], 100);
    }

    @Override
    protected ShuffleDataResult readShuffleData(ShuffleDataSegment segment) {
      blockIds.addAll(segment.getBufferSegments().stream().map(x -> x.getBlockId()).collect(Collectors.toList()));
      return new ShuffleDataResult(new byte[1], Arrays.asList(new BufferSegment(1, 1, 1, 1, 1, 1)));
    }
  }

  public static FakeHandler createFakeHandler() {
    Roaring64NavigableMap expectBlockIds = Roaring64NavigableMap.bitmapOf();
    List<ShuffleDataSegment> shuffleDataSegments = new ArrayList<>(100);
    for (int i = 0; i < 100; i++) {
      BufferSegment buf = new BufferSegment(i, 0, 0, 0, 0, 0);
      ShuffleDataSegment segment = new ShuffleDataSegment(1, 1, Arrays.asList(buf));
      shuffleDataSegments.add(segment);
      expectBlockIds.addLong(i);
    }
    Roaring64NavigableMap processBlockIds = Roaring64NavigableMap.bitmapOf();

    FakeHandler handler =
        new FakeHandler(expectBlockIds, processBlockIds, new FakeSegmentSplitter(shuffleDataSegments));
    return handler;
  }
}
