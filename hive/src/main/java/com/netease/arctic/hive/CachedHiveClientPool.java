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

package com.netease.arctic.hive;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.netease.arctic.ams.api.properties.CatalogMetaProperties;
import com.netease.arctic.table.TableMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.thrift.TException;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class CachedHiveClientPool implements ClientPool<HiveMetaStoreClient, TException> {

  private static Cache<TableMetaStore, ArcticHiveClientPool> clientPoolCache;

  private final TableMetaStore tableMetaStore;
  private final int clientPoolSize;
  private final long evictionInterval;

  public CachedHiveClientPool(TableMetaStore tableMetaStore, Map<String, String> properties) {
    this.tableMetaStore = tableMetaStore;
    this.clientPoolSize = PropertyUtil.propertyAsInt(properties,
        CatalogMetaProperties.CLIENT_POOL_SIZE,
        CatalogMetaProperties.CLIENT_POOL_SIZE_DEFAULT);
    this.evictionInterval = PropertyUtil.propertyAsLong(properties,
        CatalogMetaProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
        CatalogMetaProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_DEFAULT);
    init();
  }

  private ArcticHiveClientPool clientPool() {
    return clientPoolCache.get(tableMetaStore, k -> new ArcticHiveClientPool(tableMetaStore, clientPoolSize));
  }

  private synchronized void init() {
    if (clientPoolCache == null) {
      clientPoolCache = Caffeine.newBuilder().expireAfterAccess(evictionInterval, TimeUnit.MILLISECONDS)
          .removalListener((key, value, cause) -> ((ArcticHiveClientPool) value).close())
          .build();
    }
  }

  @Override
  public <R> R run(Action<R, HiveMetaStoreClient, TException> action) throws TException, InterruptedException {
    return clientPool().run(action);
  }
}
