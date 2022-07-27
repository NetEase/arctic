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

package com.netease.arctic.flink.table.descriptors;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.description.TextElement.text;

/**
 * Validate arctic table properties.
 */
public class ArcticValidator extends ConnectorDescriptorValidator {

  public static final String ARCTIC_EMIT_LOG = "log";
  public static final String ARCTIC_EMIT_FILE = "file";

  public static final String ARCTIC_EMIT_MODE = "arctic.emit.mode";

  public static final String ARCTIC_READ_FILE = "file";
  public static final String ARCTIC_READ_LOG = "log";

  public static final String ARCTIC_READ_MODE = "arctic.read.mode";
  public static final String ARCTIC_READ_MODE_DEFAULT = ARCTIC_READ_FILE;

  public static final String ARCTIC_LATENCY_METRIC_ENABLE = "metrics.event-latency.enable";
  public static final boolean ARCTIC_LATENCY_METRIC_ENABLE_DEFAULT = false;
  public static final String ARCTIC_THROUGHPUT_METRIC_ENABLE = "metrics.enable";
  public static final boolean ARCTIC_THROUGHPUT_METRIC_ENABLE_DEFAULT = false;
  public static final String BASE_WRITE_LOCATION = "base.write.location";
  public static final String BASE_WRITE_LOCATION_SUFFIX = "/init";

  public static final String ARCTIC_WRITE_MAX_OPEN_FILE_SIZE = "write.open-files.size.max";
  public static final long ARCTIC_WRITE_MAX_OPEN_FILE_SIZE_DEFAULT = 671088640L; // 640M = 5 * 128M

  // log.consumer.changelog.mode
  public static final String LOG_CONSUMER_CHANGELOG_MODE_APPEND_ONLY = "append-only";
  public static final String LOG_CONSUMER_CHANGELOG_MODE_ALL_KINDS = "all-kinds";

  // file scan startup mode
  public static final String FILE_SCAN_STARTUP_MODE_EARLIEST = "earliest";
  public static final String FILE_SCAN_STARTUP_MODE_LATEST = "latest";

  public static final ConfigOption<Boolean> ARCTIC_LOG_CONSISTENCY_GUARANTEE_ENABLE =
      ConfigOptions.key("log.consistency.guarantee.enable")
          .booleanType()
          .defaultValue(false)
          .withDescription("Flag hidden kafka read retraction enable or not.");

  public static final ConfigOption<String> ARCTIC_LOG_CONSUMER_CHANGELOG_MODE =
      ConfigOptions.key("log.consumer.changelog.modes")
          .stringType()
          .defaultValue("all-kinds")
          .withDescription(
              Description.builder()
                  .text("Describe what changelog modes does the log consumer support ")
                  .list(
                      text("'all-kinds' (log consumer support +I/-D/-U/+U)"),
                      text("'append-only' (log consumer only support +I)")
                  )
                  .build())
          .withDescription("Describe what changelog modes does the log consumer support.");

  public static final ConfigOption<Integer> SOURCE_READER_FETCH_BATCH_RECORD_COUNT = ConfigOptions
      .key("table.exec.iceberg.fetch-batch-record-count")
      .intType()
      .defaultValue(2048)
      .withDescription("The target number of records for Iceberg reader fetch batch.");

  public static final ConfigOption<String> FILE_SCAN_STARTUP_MODE = ConfigOptions
      .key("scan.startup.mode")
      .stringType()
      .defaultValue(FILE_SCAN_STARTUP_MODE_EARLIEST)
      .withDescription("Optional startup mode for arctic source enumerator, valid enumerations are " +
          "\"earliest\" or \"latest\", \"earliest\": read earliest table data including base and change files from" +
          " the current snapshot, \"latest\": read all incremental data in the change table starting from the" +
          " current snapshot (the current snapshot will be excluded).");

  public static final ConfigOption<Boolean> USING_CUSTOM_WATERMARK_STRATEGY = ConfigOptions
      .key("source.using.custom.watermark-strategy").booleanType().defaultValue(false)
      .withDescription("Indicates whether to apply a custom watermark strategy to the source scan." +
          " Only primary key tables are supported. Default value is false. If true, Arctic source will fetch all data" +
          " and emit the same lower watermark (1970-01-01 00:00:00 GMT) during in static source reading, then emit" +
          " the continuous watermark with process time during incremental reading.");

  @Override
  public void validate(DescriptorProperties properties) {
    String emitMode = properties.getString(ARCTIC_EMIT_MODE);
    if (StringUtils.isBlank(emitMode)) {
      throw new ValidationException(
          "None value for property '" +
              ARCTIC_EMIT_MODE);
    }

    List<String> modeList = Arrays.asList(ARCTIC_EMIT_FILE, ARCTIC_EMIT_LOG);
    for (String mode : emitMode.split(",")) {
      if (!modeList.contains(mode)) {
        throw new ValidationException(
            "Unknown value for property '" +
                ARCTIC_EMIT_MODE +
                "'.\n" +
                "Supported values are " +
                modeList.stream().collect(Collectors.toMap(v -> v, v -> DescriptorProperties.noValidation())).keySet() +
                " but was: " +
                mode);
      }
    }
  }

  public static Configuration asConfiguration(Map<String, String> options) {
    final Configuration configuration = new Configuration();
    options.forEach(configuration::setString);
    return configuration;
  }

  private static RowType getRowType(CatalogBaseTable flinkTable) {
    return (RowType) flinkTable.getSchema().toRowDataType().getLogicalType();
  }
}
