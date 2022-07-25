/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netease.arctic.flink.read.hybrid.enumerator;

import com.netease.arctic.flink.read.FlinkSplitPlanner;
import com.netease.arctic.flink.read.hybrid.split.ArcticSplit;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * The result that contains {@link ArcticSplit}s and is generated by {@link FlinkSplitPlanner}.
 */
public class ContinuousEnumerationResult {
  public static final ContinuousEnumerationResult EMPTY =
      new ContinuousEnumerationResult(Collections.emptyList(), null, ArcticEnumeratorOffset.EMPTY);

  private final Collection<ArcticSplit> splits;
  private final ArcticEnumeratorOffset fromOffset;
  private final ArcticEnumeratorOffset toOffset;

  /**
   * @param splits     should never be null. But it can be an empty collection
   * @param fromOffset can be null
   * @param toOffset   should never be null. But it can have null snapshotId and snapshotTimestampMs
   */
  public ContinuousEnumerationResult(
      Collection<ArcticSplit> splits,
      ArcticEnumeratorOffset fromOffset,
      ArcticEnumeratorOffset toOffset) {
    Preconditions.checkArgument(splits != null, "Invalid to splits collection: null");
    Preconditions.checkArgument(toOffset != null, "Invalid end position: null");
    this.splits = splits;
    this.fromOffset = fromOffset;
    this.toOffset = toOffset;
  }

  public Collection<ArcticSplit> splits() {
    return splits;
  }

  public ArcticEnumeratorOffset fromOffset() {
    return fromOffset;
  }

  public ArcticEnumeratorOffset toOffset() {
    return toOffset;
  }

  public boolean isEmpty() {
    return null == splits || splits.isEmpty();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("splits", Arrays.toString(splits.toArray()))
        .add("fromPosition", fromOffset)
        .add("toPosition", toOffset)
        .toString();
  }
}
