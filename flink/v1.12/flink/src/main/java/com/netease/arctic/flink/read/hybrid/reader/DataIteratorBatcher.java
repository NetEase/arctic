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

package com.netease.arctic.flink.read.hybrid.reader;

import com.netease.arctic.flink.read.source.DataIterator;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.iceberg.io.CloseableIterator;

import java.io.Serializable;

/**
 * Batcher converts iterator of T into iterator of batched {@code RecordsWithSplitIds<RecordAndPosition<T>>},
 * as FLIP-27's {@link SplitReader#fetch()} returns batched records.
 */
@FunctionalInterface
public interface DataIteratorBatcher<T> extends Serializable {
  CloseableIterator<RecordsWithSplitIds<ArcticRecordWithOffset<T>>> batch(
      String splitId, DataIterator<T> inputIterator);
}
