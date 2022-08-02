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

package com.netease.arctic.ams.api.properties;

import java.util.concurrent.TimeUnit;

public class CatalogMetaProperties {
    public static final String STORAGE_CONFIGS_KEY_TYPE = "storage.type" ;
    public static final String STORAGE_CONFIGS_KEY_HDFS_SITE = "hadoop.hdfs.site" ;
    public static final String STORAGE_CONFIGS_KEY_CORE_SITE = "hadoop.core.site" ;
    public static final String STORAGE_CONFIGS_KEY_HIVE_SITE = "hive.site" ;

    public static final String STORAGE_CONFIGS_VALUE_TYPE_HDFS = "hdfs" ;

    public static final String AUTH_CONFIGS_KEY_TYPE = "auth.type" ;
    public static final String AUTH_CONFIGS_KEY_PRINCIPAL = "auth.kerberos.principal" ;
    public static final String AUTH_CONFIGS_KEY_KEYTAB = "auth.kerberos.keytab" ;
    public static final String AUTH_CONFIGS_KEY_KRB5 = "auth.kerberos.krb5" ;
    public static final String AUTH_CONFIGS_KEY_HADOOP_USERNAME = "auth.simple.hadoop_username" ;

    public static final String AUTH_CONFIGS_VALUE_TYPE_SIMPLE = "simple";
    public static final String AUTH_CONFIGS_VALUE_TYPE_KERBEROS = "kerberos" ;

    public static final String KEY_WAREHOUSE_DIR = "warehouse.dir";

    public static final String CATALOG_TYPE_HADOOP = "hadoop";

    public static final String CLIENT_POOL_SIZE = "clients";
    public static final int CLIENT_POOL_SIZE_DEFAULT = 2;

    public static final String CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS = "client.pool.cache.eviction-interval-ms";
    public static final long CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_DEFAULT = TimeUnit.MINUTES.toMillis(5);
}
