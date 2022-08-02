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

package com.netease.arctic.hive.catalog;

import com.netease.arctic.AmsClient;
import com.netease.arctic.ams.api.CatalogMeta;
import com.netease.arctic.ams.api.TableMeta;
import com.netease.arctic.catalog.BaseArcticCatalog;
import com.netease.arctic.hive.CachedHiveClientPool;
import com.netease.arctic.hive.table.KeyedHiveTable;
import com.netease.arctic.hive.table.UnkeyedHiveTable;
import com.netease.arctic.table.TableBuilder;
import com.netease.arctic.table.TableIdentifier;
import com.netease.arctic.table.UnkeyedTable;
import org.apache.iceberg.Schema;

import java.util.List;
import java.util.Map;

public class ArcticHiveCatalog extends BaseArcticCatalog {

  private CachedHiveClientPool hiveClientPool;

  @Override
  public void initialize(
      AmsClient client, CatalogMeta meta, Map<String, String> properties) {
    super.initialize(client, meta, properties);
    this.hiveClientPool = new CachedHiveClientPool(tableMetaStore, properties);
  }

  @Override
  public List<String> listDatabases() {
    //TODO list databases from hms
    return null;
  }

  @Override
  public void createDatabase(String databaseName) {
    //TODO create database in hms
  }

  @Override
  public void dropDatabase(String databaseName) {
    //TODO drop database from hms
  }

  @Override
  public List<TableIdentifier> listTables(String database) {
    //TODO list tables from hms and ams
    return null;
  }

  @Override
  protected void doDropTable(TableMeta meta, boolean purge) {
    //TODO drop table from hms and ams
  }

  @Override
  public TableBuilder newTableBuilder(
      TableIdentifier identifier, Schema schema) {
    return new ArcticHiveTableBuilder(identifier, schema);
  }

  @Override
  protected KeyedHiveTable loadKeyedTable(TableMeta tableMeta) {
    //TODO load keyed hive table from ams
    return null;
  }

  @Override
  protected UnkeyedHiveTable loadUnKeyedTable(TableMeta tableMeta) {
    //TODO load unkeyed hive table from ams
    return null;
  }

  class ArcticHiveTableBuilder extends BaseArcticTableBuilder {

    public ArcticHiveTableBuilder(TableIdentifier identifier, Schema schema) {
      super(identifier, schema);
    }

    @Override
    protected KeyedHiveTable createKeyedTable(TableMeta meta) {
      //TODO create keyed hive table
      return null;
    }

    @Override
    protected UnkeyedTable createUnKeyedTable(TableMeta meta) {
      //TODO create unkeyed hive table
      return null;
    }
  }

}
