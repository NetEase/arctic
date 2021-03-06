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

package com.netease.arctic.flink.catalog;

import com.netease.arctic.flink.FlinkTestBase;
import com.netease.arctic.table.TableIdentifier;
import org.apache.flink.util.CollectionUtil;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static com.netease.arctic.ams.api.MockArcticMetastoreServer.TEST_CATALOG_NAME;

public class TestCatalog  extends FlinkTestBase {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private static final String DB = PK_TABLE_ID.getDatabase();
  private static final String TABLE = "test_keyed";
  private static final String TOPIC = String.join(".", TEST_CATALOG_NAME, DB, TABLE);

  public void before() {
    super.before();
    super.config();
  }

  @Test
  public void testDDL() throws IOException {
    sql("CREATE CATALOG arcticCatalog WITH %s", toWithClause(props));
    sql("USE CATALOG arcticCatalog");

    sql("CREATE TABLE arcticCatalog." + DB + "." + TABLE +
        " (" +
        " id INT," +
        " name STRING," +
        " t TIMESTAMP," +
        " PRIMARY KEY (id) NOT ENFORCED " +
        ") PARTITIONED BY(t) " +
        " WITH (" +
        " 'connector' = 'arctic'," +
        " 'location' = '" + tableDir.getAbsolutePath() + "'" +
        ")");
    sql("SHOW tables");

    Assert.assertTrue(testCatalog.loadTable(TableIdentifier.of(TEST_CATALOG_NAME, DB, TABLE)).isKeyedTable());
    sql("DROP TABLE " + DB + "." + TABLE);

    sql("DROP DATABASE " + DB);

    Assert.assertTrue(CollectionUtil.isNullOrEmpty(testCatalog.listDatabases()));
    sql("DROP CATALOG arcticCatalog");
  }

}
