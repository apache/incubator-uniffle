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

package org.apache.uniffle.common.util;

import java.io.IOException;
import java.util.function.LongConsumer;
import java.util.stream.LongStream;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class RoaringBitmapBlockIdSet implements BlockIdSet {
  public final Roaring64NavigableMap bitmap;

  public RoaringBitmapBlockIdSet() {
    this(Roaring64NavigableMap.bitmapOf());
  }

  public RoaringBitmapBlockIdSet(Roaring64NavigableMap bitmap) {
    this.bitmap = bitmap;
  }

  @Override
  public synchronized BlockIdSet add(long blockId) {
    bitmap.addLong(blockId);
    return this;
  }

  @Override
  public synchronized BlockIdSet addAll(BlockIdSet blockIds) {
    if (!(blockIds instanceof RoaringBitmapBlockIdSet)) {
      throw new UnsupportedOperationException(
          "Only supported for RoaringBitmapBlockIdSet arguments");
    }
    bitmap.or(((RoaringBitmapBlockIdSet) blockIds).bitmap);
    return this;
  }

  @Override
  public synchronized BlockIdSet remove(long blockId) {
    bitmap.removeLong(blockId);
    return this;
  }

  @Override
  public synchronized BlockIdSet removeAll(BlockIdSet blockIds) {
    if (!(blockIds instanceof RoaringBitmapBlockIdSet)) {
      throw new UnsupportedOperationException(
          "Only supported for RoaringBitmapBlockIdSet arguments");
    }
    bitmap.andNot(((RoaringBitmapBlockIdSet) blockIds).bitmap);
    return this;
  }

  @Override
  public synchronized BlockIdSet retainAll(BlockIdSet blockIds) {
    if (!(blockIds instanceof RoaringBitmapBlockIdSet)) {
      throw new UnsupportedOperationException(
          "Only supported for RoaringBitmapBlockIdSet arguments");
    }
    bitmap.and(((RoaringBitmapBlockIdSet) blockIds).bitmap);
    return this;
  }

  @Override
  public boolean contains(long blockId) {
    return bitmap.contains(blockId);
  }

  @Override
  public boolean containsAll(BlockIdSet blockIds) {
    if (!(blockIds instanceof RoaringBitmapBlockIdSet)) {
      throw new UnsupportedOperationException(
          "Only supported for RoaringBitmapBlockIdSet arguments");
    }
    Roaring64NavigableMap expecteds = ((RoaringBitmapBlockIdSet) blockIds).bitmap;

    // first a quick check: no need for expensive bitwise AND when this is equal to the other BlockIdSet
    if (this.bitmap.equals(expecteds)) {
      return true;
    }

    // bitmaps are not equal, check if all expected bits are contained
    Roaring64NavigableMap actuals = RssUtils.cloneBitMap(bitmap);
    actuals.and(expecteds);
    return actuals.equals(expecteds);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof BlockIdSet)) {
      return false;
    }

    BlockIdSet blockIds = (BlockIdSet) other;
    if (!(blockIds instanceof RoaringBitmapBlockIdSet)) {
      throw new UnsupportedOperationException(
          "Only supported for RoaringBitmapBlockIdSet arguments");
    }

    return bitmap.equals(((RoaringBitmapBlockIdSet) blockIds).bitmap);
  }

  @Override
  public int getIntCardinality() {
    return bitmap.getIntCardinality();
  }

  @Override
  public long getLongCardinality() {
    return bitmap.getLongCardinality();
  }

  @Override
  public boolean isEmpty() {
    return bitmap.isEmpty();
  }

  @Override
  public synchronized void forEach(LongConsumer func) {
    bitmap.forEach(func::accept);
  }

  @Override
  public LongStream stream() {
    return bitmap.stream();
  }

  @Override
  public synchronized BlockIdSet copy() {
    return new RoaringBitmapBlockIdSet(RssUtils.cloneBitMap(bitmap));
  }

  @Override
  public byte[] serialize() throws IOException {
    return RssUtils.serializeBitMap(bitmap);
  }

  @Override
  public String toString() {
    return bitmap.toString();
  }
}
