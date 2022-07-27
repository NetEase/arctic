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

package com.netease.arctic.flink.read.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.data.RowData;

import java.sql.Date;

public class IncrementalWatermarkStrategy implements WatermarkStrategy<RowData> {
  private static final long serialVersionUID = 141960275254590513L;

  @Override
  public WatermarkGenerator<RowData> createWatermarkGenerator(
      WatermarkGeneratorSupplier.Context context) {

    return new ArcticWatermarkGenerator();
  }

  static class ArcticWatermarkGenerator implements WatermarkGenerator<RowData> {
    int count = 0;
    long lowestTimestamp = new Date(0).getTime();

    @Override
    public void onEvent(RowData event, long eventTimestamp, WatermarkOutput output) {
      count++;
      if (count > 20) {
        return;
      }
      output.emitWatermark(new Watermark(lowestTimestamp));
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
      if (count > 20) {
        output.emitWatermark(new Watermark(System.currentTimeMillis()));
      }
    }
  }

}
