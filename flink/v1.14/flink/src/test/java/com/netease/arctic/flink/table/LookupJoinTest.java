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

import com.netease.arctic.flink.FlinkTestBase;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class LookupJoinTest extends FlinkTestBase {

  private static final String DB = PK_TABLE_ID.getDatabase();
  private static final String TABLE = "test_keyed";

  @Override
  public void before() {
    super.before();
    super.config();
  }

  @Test
  public void testLookupJoinWithInit() throws Exception {
    StreamTableEnvironment tableEnv = null;
    String arcticTableName = null;
    try {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      // enable checkpoint
      env.enableCheckpointing(3000);
      env.setParallelism(1);

      tableEnv = StreamTableEnvironment.create(env, EnvironmentSettings
          .newInstance()
          .inStreamingMode().build());

      tableEnv.executeSql("create table left_view (id bigint, opt timestamp(3), watermark for opt as opt," +
          " primary key (id) not enforced) with ('connector'='bounded_source')");

      tableEnv.executeSql(String.format("CREATE CATALOG arcticCatalog WITH %s", toWithClause(props)));
      Map<String, String> tableProperties = new HashMap<>();
      tableProperties.put("arctic.read.mode", "file");
      arcticTableName = String.format("arcticCatalog.%s.%s", DB, TABLE);
//    tableEnv.executeSql(String.format("drop table %s", arcticTableName));
      String sql = String.format("CREATE TABLE IF NOT EXISTS %s (" +
          " id INT, name STRING, PRIMARY KEY (id) NOT ENFORCED) WITH %s", arcticTableName, toWithClause(tableProperties));
      tableEnv.executeSql(sql);

      TableResult tableResult = tableEnv.executeSql(
          String.format("select u.*, d.* from left_view as u left join %s for system_time as of u.opt as d on u.id = d.id",
              arcticTableName));

      CloseableIterator<Row> iterator = tableResult.collect();
      while (iterator.hasNext()) {
        System.out.println(iterator.next());
      }


      JobClient jobClient = tableResult.getJobClient().get();
      if (jobClient.getJobStatus().get() != JobStatus.RUNNING) {
        Thread.currentThread().sleep(100000);
        jobClient.cancel();
      }
    } finally {
      tableEnv.executeSql("drop table " + arcticTableName);
    }
  }
}
