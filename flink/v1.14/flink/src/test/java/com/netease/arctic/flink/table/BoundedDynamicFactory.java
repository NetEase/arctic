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

package com.netease.arctic.flink.table;

import com.netease.arctic.flink.util.NumberSequenceSource;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSourceFactory;

import java.util.HashSet;
import java.util.Set;

public class BoundedDynamicFactory implements DynamicTableSourceFactory {
  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    return new BoundedDynamicSource();
  }

  @Override
  public String factoryIdentifier() {
    return "bounded_source";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    final Set<ConfigOption<?>> options = new HashSet<>();
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    final Set<ConfigOption<?>> options = new HashSet<>();
    return options;
  }

  static class BoundedDynamicSource implements ScanTableSource {
    @Override
    public ChangelogMode getChangelogMode() {
      return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
      return new DataStreamScanProvider() {
        @Override
        public DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {
          return execEnv.fromSource(new NumberSequenceSource(0, 10000), WatermarkStrategy.forGenerator((WatermarkGeneratorSupplier<RowData>) context -> new WatermarkGenerator<RowData>() {
            @Override
            public void onEvent(RowData event, long eventTimestamp, WatermarkOutput output) {
              output.emitWatermark(new Watermark(System.currentTimeMillis()));
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput output) {
              output.emitWatermark(new Watermark(System.currentTimeMillis()));
            }
          }), "left table");
        }

        @Override
        public boolean isBounded() {
          return true;
        }
      };
    }

    @Override
    public DynamicTableSource copy() {
      return new BoundedDynamicSource();
    }

    @Override
    public String asSummaryString() {
      return "Bounded Source";
    }
  }

}
