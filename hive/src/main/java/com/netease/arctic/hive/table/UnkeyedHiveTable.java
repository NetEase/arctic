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

package com.netease.arctic.hive.table;

import com.netease.arctic.AmsClient;
import com.netease.arctic.io.ArcticFileIO;
import com.netease.arctic.table.BaseUnkeyedTable;
import com.netease.arctic.table.TableIdentifier;
import org.apache.iceberg.Table;

public class UnkeyedHiveTable extends BaseUnkeyedTable {
  public UnkeyedHiveTable(
      TableIdentifier tableIdentifier,
      Table icebergTable,
      ArcticFileIO arcticFileIO, AmsClient client) {
    super(tableIdentifier, icebergTable, arcticFileIO, client);
  }
}
