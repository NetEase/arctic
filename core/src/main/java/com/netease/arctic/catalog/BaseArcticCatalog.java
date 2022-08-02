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

package com.netease.arctic.catalog;

import com.netease.arctic.AmsClient;
import com.netease.arctic.NoSuchDatabaseException;
import com.netease.arctic.ams.api.AlreadyExistsException;
import com.netease.arctic.ams.api.CatalogMeta;
import com.netease.arctic.ams.api.NoSuchObjectException;
import com.netease.arctic.ams.api.TableMeta;
import com.netease.arctic.ams.api.properties.CatalogMetaProperties;
import com.netease.arctic.ams.api.properties.MetaTableProperties;
import com.netease.arctic.io.ArcticFileIO;
import com.netease.arctic.io.ArcticHadoopFileIO;
import com.netease.arctic.op.ArcticHadoopTableOperations;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.BaseKeyedTable;
import com.netease.arctic.table.BaseTable;
import com.netease.arctic.table.BaseUnkeyedTable;
import com.netease.arctic.table.ChangeTable;
import com.netease.arctic.table.KeyedTable;
import com.netease.arctic.table.PrimaryKeySpec;
import com.netease.arctic.table.TableBuilder;
import com.netease.arctic.table.TableIdentifier;
import com.netease.arctic.table.TableMetaStore;
import com.netease.arctic.table.TableProperties;
import com.netease.arctic.table.UnkeyedTable;
import com.netease.arctic.trace.CreateTableTransaction;
import com.netease.arctic.utils.ConvertStructUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Tables;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopTableOperations;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Base {@link ArcticCatalog} implementation.
 */
public class BaseArcticCatalog implements ArcticCatalog {
  private static final Logger LOG = LoggerFactory.getLogger(BaseArcticCatalog.class);

  protected AmsClient client;
  protected CatalogMeta catalogMeta;
  protected transient Tables tables;
  protected transient TableMetaStore tableMetaStore;
  private String catalogName;

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public void initialize(
      AmsClient client,
      CatalogMeta meta,
      Map<String, String> properties) {
    this.client = client;
    this.catalogMeta = meta;
    this.catalogName = meta.getCatalogName();
    if (meta.getStorageConfigs() != null &&
        CatalogMetaProperties.STORAGE_CONFIGS_VALUE_TYPE_HDFS.equalsIgnoreCase(
            meta.getStorageConfigs().get(CatalogMetaProperties.STORAGE_CONFIGS_KEY_TYPE))) {
      if (!meta.getStorageConfigs().containsKey(CatalogMetaProperties.STORAGE_CONFIGS_KEY_HDFS_SITE)) {
        throw new IllegalStateException("lack hdfs.site config");
      }
      if (!meta.getStorageConfigs().containsKey(CatalogMetaProperties.STORAGE_CONFIGS_KEY_CORE_SITE)) {
        throw new IllegalStateException("lack core.site config");
      }
    }

    TableMetaStore.Builder builder = getMetaStoreBuilder();
    tableMetaStore = builder.build();
    tables = new HadoopTables(tableMetaStore.getConfiguration());
  }

  @Override
  public List<String> listDatabases() {
    try {
      return client.getDatabases(this.catalogName);
    } catch (TException e) {
      throw new IllegalStateException("failed load database", e);
    }
  }

  @Override
  public void createDatabase(String databaseName) {
    try {
      client.createDatabase(this.catalogName, databaseName);
    } catch (AlreadyExistsException e) {
      throw new org.apache.iceberg.exceptions.AlreadyExistsException("Database already exists, %s", databaseName);
    } catch (TException e) {
      throw new IllegalStateException("failed create database", e);
    }
  }

  @Override
  public void dropDatabase(String databaseName) {
    try {
      client.dropDatabase(this.catalogName, databaseName);
    } catch (NoSuchObjectException e0) {
      throw new NoSuchDatabaseException(e0, databaseName);
    } catch (TException e) {
      throw new IllegalStateException("failed drop database", e);
    }
  }

  @Override
  public List<TableIdentifier> listTables(String database) {
    try {
      return client.listTables(this.catalogName, database)
          .stream()
          .map(t -> TableIdentifier.of(
              catalogName, database,
              t.getTableIdentifier().getTableName()))
          .collect(Collectors.toList());
    } catch (TException e) {
      throw new IllegalStateException("failed load tables", e);
    }
  }

  @Override
  public ArcticTable loadTable(TableIdentifier identifier) {
    if (!this.catalogName.equals(identifier.getCatalog())) {
      throw new IllegalArgumentException("catalog name miss match");
    }
    TableMeta meta = getArcticTableMeta(identifier);
    if (meta.getLocations() == null) {
      throw new IllegalStateException("load table failed, lack locations info");
    }
    return loadTableByMeta(meta);
  }

  protected ArcticTable loadTableByMeta(TableMeta meta) {
    if (meta.getKeySpec() != null &&
        meta.getKeySpec().getFields() != null &&
        meta.getKeySpec().getFields().size() > 0) {
      return loadKeyedTable(meta);
    } else {
      return loadUnKeyedTable(meta);
    }
  }

  protected KeyedTable loadKeyedTable(TableMeta tableMeta) {
    TableIdentifier tableIdentifier = TableIdentifier.of(tableMeta.getTableIdentifier());
    String tableLocation = checkLocation(tableMeta, MetaTableProperties.LOCATION_KEY_TABLE);
    String baseLocation = checkLocation(tableMeta, MetaTableProperties.LOCATION_KEY_BASE);
    String changeLocation = checkLocation(tableMeta, MetaTableProperties.LOCATION_KEY_CHANGE);

    ArcticFileIO fileIO = new ArcticHadoopFileIO(tableMetaStore);
    Table baseIcebergTable = tableMetaStore.doAs(() -> tables.load(baseLocation));
    BaseTable baseTable = new BaseKeyedTable.BaseInternalTable(tableIdentifier,
        useArcticTableOperations(baseIcebergTable, baseLocation, fileIO, tableMetaStore.getConfiguration()),
        fileIO, client);

    Table changeIcebergTable = tableMetaStore.doAs(() -> tables.load(changeLocation));
    ChangeTable changeTable = new BaseKeyedTable.ChangeInternalTable(tableIdentifier,
        useArcticTableOperations(changeIcebergTable, changeLocation, fileIO, tableMetaStore.getConfiguration()),
        fileIO, client);
    return new BaseKeyedTable(tableMeta, tableLocation,
        buildPrimaryKeySpec(baseTable.schema(), tableMeta), client, baseTable, changeTable);
  }

  protected PrimaryKeySpec buildPrimaryKeySpec(Schema schema, TableMeta tableMeta) {
    PrimaryKeySpec.Builder builder = PrimaryKeySpec.builderFor(schema);
    if (tableMeta.getKeySpec() != null &&
        tableMeta.getKeySpec().getFields() != null &&
        tableMeta.getKeySpec().getFields().size() > 0) {
      for (String field : tableMeta.getKeySpec().getFields()) {
        builder.addColumn(field);
      }
    }
    return builder.build();
  }

  protected UnkeyedTable loadUnKeyedTable(TableMeta tableMeta) {
    TableIdentifier tableIdentifier = TableIdentifier.of(tableMeta.getTableIdentifier());
    String baseLocation = checkLocation(tableMeta, MetaTableProperties.LOCATION_KEY_BASE);
    Table table = tableMetaStore.doAs(() -> tables.load(baseLocation));
    ArcticFileIO arcticFileIO = new ArcticHadoopFileIO(tableMetaStore);
    return new BaseUnkeyedTable(tableIdentifier, useArcticTableOperations(table, baseLocation,
        arcticFileIO, tableMetaStore.getConfiguration()), arcticFileIO, client);
  }

  private String checkLocation(TableMeta meta, String locationKey) {
    String location = meta.getLocations().get(locationKey);
    Preconditions.checkArgument(StringUtils.isNotBlank(location), "table location can't found");
    return location;
  }

  private Table useArcticTableOperations(
      Table table, String tableLocation,
      ArcticFileIO arcticFileIO, Configuration configuration) {
    if (table instanceof org.apache.iceberg.BaseTable) {
      org.apache.iceberg.BaseTable baseTable = (org.apache.iceberg.BaseTable) table;
      if (baseTable.operations() instanceof HadoopTableOperations) {
        return new org.apache.iceberg.BaseTable(new ArcticHadoopTableOperations(new Path(tableLocation),
            arcticFileIO, configuration), table.name());
      }
    }
    return table;
  }

  @Override
  public void renameTable(TableIdentifier from, String newTableName) {
    throw new UnsupportedOperationException("unsupported rename arctic table for now.");
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    TableMeta meta;
    try {
      meta = getArcticTableMeta(identifier);
    } catch (NoSuchTableException e) {
      return false;
    }

    doDropTable(meta, purge);
    return true;
  }

  protected void doDropTable(TableMeta meta, boolean purge) {

    try {
      client.removeTable(meta.getTableIdentifier(), purge);
    } catch (TException e) {
      throw new IllegalStateException("error when delete table metadata from metastore");
    }

    String baseLocation = meta.getLocations().get(MetaTableProperties.LOCATION_KEY_BASE);
    String changeLocation = meta.getLocations().get(MetaTableProperties.LOCATION_KEY_CHANGE);

    try {
      if (StringUtils.isNotBlank(baseLocation)) {
        dropInternalTable(tableMetaStore, baseLocation, purge);
      }
      if (StringUtils.isNotBlank(changeLocation)) {
        dropInternalTable(tableMetaStore, changeLocation, purge);
      }
    } catch (Exception e) {
      LOG.warn("drop base/change iceberg table fail ", e);
    }
  }

  @Override
  public TableBuilder newTableBuilder(TableIdentifier identifier, Schema schema) {
    return new BaseArcticTableBuilder(identifier, schema);
  }

  public TableMetaStore getTableMetaStore() {
    return tableMetaStore;
  }

  private TableMeta getArcticTableMeta(TableIdentifier identifier) {
    TableMeta meta;
    try {
      meta = client.getTable(identifier.buildTableIdentifier());
      return meta;
    } catch (NoSuchObjectException e) {
      throw new NoSuchTableException(e, "load table failed %s.", identifier);
    } catch (TException e) {
      throw new IllegalStateException(String.format("failed load table %s.", identifier), e);
    }
  }

  protected TableMetaStore.Builder getMetaStoreBuilder() {
    TableMetaStore.Builder builder = TableMetaStore.builder();
    if (this.catalogMeta.getStorageConfigs() != null) {
      Map<String, String> storageConfigs = this.catalogMeta.getStorageConfigs();
      if (CatalogMetaProperties.STORAGE_CONFIGS_VALUE_TYPE_HDFS
          .equalsIgnoreCase(
              storageConfigs.get(CatalogMetaProperties.STORAGE_CONFIGS_KEY_TYPE))) {
        String coreSite = storageConfigs.get(CatalogMetaProperties.STORAGE_CONFIGS_KEY_CORE_SITE);
        String hdfsSite = storageConfigs.get(CatalogMetaProperties.STORAGE_CONFIGS_KEY_HDFS_SITE);
        builder.withBase64CoreSite(coreSite)
            .withBase64HdfsSite(hdfsSite);
      }
    }
    if (this.catalogMeta.getAuthConfigs() != null) {
      Map<String, String> authConfigs = this.catalogMeta.getAuthConfigs();
      String authType = authConfigs.get(CatalogMetaProperties.AUTH_CONFIGS_KEY_TYPE);
      if (CatalogMetaProperties.AUTH_CONFIGS_VALUE_TYPE_SIMPLE.equalsIgnoreCase(authType)) {
        String hadoopUsername = authConfigs.get(CatalogMetaProperties.AUTH_CONFIGS_KEY_HADOOP_USERNAME);
        builder.withSimpleAuth(hadoopUsername);
      } else if (CatalogMetaProperties.AUTH_CONFIGS_VALUE_TYPE_KERBEROS.equalsIgnoreCase(authType)) {
        String krb5 = authConfigs.get(CatalogMetaProperties.AUTH_CONFIGS_KEY_KRB5);
        String keytab = authConfigs.get(CatalogMetaProperties.AUTH_CONFIGS_KEY_KEYTAB);
        String principal = authConfigs.get(CatalogMetaProperties.AUTH_CONFIGS_KEY_PRINCIPAL);
        builder.withBase64KrbAuth(keytab, krb5, principal);
      }
    }
    return builder;
  }

  private void dropInternalTable(TableMetaStore tableMetaStore, String internalTableLocation, boolean purge) {
    final HadoopTables internalTables = new HadoopTables(tableMetaStore.getConfiguration());
    tableMetaStore.doAs(() -> {
      internalTables.dropTable(internalTableLocation, purge);
      return null;
    });
  }

  protected class BaseArcticTableBuilder implements TableBuilder {
    protected TableIdentifier identifier;
    protected Schema schema;
    protected PartitionSpec partitionSpec;
    protected SortOrder sortOrder;
    protected Map<String, String> properties = new HashMap<>();
    protected PrimaryKeySpec primaryKeySpec = PrimaryKeySpec.noPrimaryKey();
    protected String location;

    public BaseArcticTableBuilder(TableIdentifier identifier, Schema schema) {
      this.identifier = identifier;
      this.schema = schema;
    }

    @Override
    public TableBuilder withPartitionSpec(PartitionSpec partitionSpec) {
      this.partitionSpec = partitionSpec;
      return this;
    }

    @Override
    public TableBuilder withSortOrder(SortOrder sortOrder) {
      this.sortOrder = sortOrder;
      return this;
    }

    @Override
    public TableBuilder withProperties(Map<String, String> properties) {
      this.properties.putAll(properties);
      return this;
    }

    @Override
    public TableBuilder withProperty(String key, String value) {
      this.properties.put(key, value);
      return this;
    }

    @Override
    public TableBuilder withPrimaryKeySpec(PrimaryKeySpec primaryKeySpec) {
      this.primaryKeySpec = primaryKeySpec;
      return this;
    }

    @Override
    public ArcticTable create() {
      doCreateCheck();
      ConvertStructUtil.TableMetaBuilder builder = createTableMataBuilder();
      TableMeta meta = builder.build();
      ArcticTable table = doCreateTable(meta);
      createTableMeta(meta);
      return table;
    }

    public Transaction newCreateTableTransaction() {
      ArcticFileIO arcticFileIO = new ArcticHadoopFileIO(tableMetaStore);
      ConvertStructUtil.TableMetaBuilder builder = createTableMataBuilder();
      TableMeta meta = builder.build();
      String location = getTableLocationForCreate();
      TableOperations tableOperations = new ArcticHadoopTableOperations(new Path(location),
          arcticFileIO, tableMetaStore.getConfiguration());
      TableMetadata tableMetadata = tableMetadata(schema, partitionSpec, sortOrder, properties, location);
      Transaction transaction =
          Transactions.createTableTransaction(identifier.getTableName(), tableOperations, tableMetadata);
      return new CreateTableTransaction(
          transaction,
          this::create,
          () -> {
            doRollbackCreateTable(meta);
            try {
              client.removeTable(
                  identifier.buildTableIdentifier(),
                  true);
            } catch (TException e) {
              throw new RuntimeException(e);
            }
          }
      );
    }

    protected void doCreateCheck() {
      listDatabases().stream()
          .filter(d -> d.equals(identifier.getDatabase()))
          .findFirst()
          .orElseThrow(() -> new NoSuchDatabaseException(identifier.getDatabase()));

      try {
        client.getTable(identifier.buildTableIdentifier());
        throw new org.apache.iceberg.exceptions.AlreadyExistsException("table already exist");
      } catch (NoSuchObjectException e) {
        checkProperties();
      } catch (TException e) {
        throw new IllegalStateException("failed when load table", e);
      }
    }

    protected void checkProperties() {
      boolean enableStream = PropertyUtil.propertyAsBoolean(properties,
          TableProperties.ENABLE_LOG_STORE, TableProperties.ENABLE_LOG_STORE_DEFAULT);
      if (enableStream) {
        Preconditions.checkArgument(properties.containsKey(TableProperties.LOG_STORE_MESSAGE_TOPIC),
            "log-store.topic must not be null when log-store.enable is true.");
        Preconditions.checkArgument(properties.containsKey(TableProperties.LOG_STORE_ADDRESS),
            "log-store.address must not be null when log-store.enable is true.");
        String logStoreType = properties.get(TableProperties.LOG_STORE_TYPE);
        Preconditions.checkArgument(logStoreType == null ||
                logStoreType.equals(TableProperties.LOG_STORE_STORAGE_TYPE_DEFAULT),
            "log-store.type support only kafka.");
        properties.putIfAbsent(TableProperties.LOG_STORE_DATA_FORMAT, TableProperties.LOG_STORE_DATA_FORMAT_DEFAULT);
      }
    }

    protected ArcticTable doCreateTable(TableMeta meta) {
      ArcticTable table;
      if (primaryKeySpec.primaryKeyExisted()) {
        table = createKeyedTable(meta);
      } else {
        table = createUnKeyedTable(meta);
      }
      return table;
    }

    protected void createTableMeta(TableMeta meta) {
      boolean tableCreated = false;
      try {
        client.createTableMeta(meta);
        tableCreated = true;
      } catch (AlreadyExistsException e) {
        throw new org.apache.iceberg.exceptions.AlreadyExistsException("table already exist", e);
      } catch (TException e) {
        throw new IllegalStateException("update table meta failed", e);
      } finally {
        if (!tableCreated) {
          doRollbackCreateTable(meta);
        }
      }
    }

    protected void doRollbackCreateTable(TableMeta meta) {
      final String baseLocation = meta.getLocations().get(MetaTableProperties.LOCATION_KEY_BASE);
      final String changeLocation = meta.getLocations().get(MetaTableProperties.LOCATION_KEY_CHANGE);
      if (StringUtils.isNotBlank(baseLocation)) {
        try {
          dropInternalTable(tableMetaStore, baseLocation, true);
        } catch (Exception e) {
          LOG.warn("error when rollback internal table", e);
        }
      }
      if (StringUtils.isNotBlank(changeLocation)) {
        try {
          dropInternalTable(tableMetaStore, changeLocation, true);
        } catch (Exception e) {
          LOG.warn("error when rollback internal table", e);
        }
      }
    }

    protected ConvertStructUtil.TableMetaBuilder createTableMataBuilder() {
      ConvertStructUtil.TableMetaBuilder builder = ConvertStructUtil.newTableMetaBuilder(
          this.identifier, this.schema);
      String tableLocation = getTableLocationForCreate();
      builder.withTableLocation(tableLocation)
          .withProperties(this.properties)
          .withPrimaryKeySpec(this.primaryKeySpec);

      if (this.primaryKeySpec.primaryKeyExisted()) {
        builder = builder
            .withBaseLocation(tableLocation + "/base")
            .withChangeLocation(tableLocation + "/change");
      } else {
        builder = builder.withBaseLocation(tableLocation);
      }
      return builder;
    }

    protected KeyedTable createKeyedTable(TableMeta meta) {
      TableIdentifier tableIdentifier = TableIdentifier.of(meta.getTableIdentifier());
      String tableLocation = checkLocation(meta, MetaTableProperties.LOCATION_KEY_TABLE);
      String baseLocation = checkLocation(meta, MetaTableProperties.LOCATION_KEY_BASE);
      String changeLocation = checkLocation(meta, MetaTableProperties.LOCATION_KEY_CHANGE);

      Map<String, String> tableProperties = meta.getProperties();
      tableProperties.put(TableProperties.TABLE_CREATE_TIME, String.valueOf(System.currentTimeMillis()));
      tableProperties.put(org.apache.iceberg.TableProperties.FORMAT_VERSION, "2");

      ArcticFileIO fileIO = new ArcticHadoopFileIO(tableMetaStore);
      Table baseIcebergTable = tableMetaStore.doAs(() -> {
        try {
          return tables.create(schema, partitionSpec, tableProperties, baseLocation);
        } catch (Exception e) {
          throw new IllegalStateException("create base table failed", e);
        }
      });

      BaseTable baseTable = new BaseKeyedTable.BaseInternalTable(tableIdentifier,
          useArcticTableOperations(baseIcebergTable, baseLocation, fileIO, tableMetaStore.getConfiguration()),
          fileIO, client);

      Table changeIcebergTable = tableMetaStore.doAs(() -> {
        try {
          return tables.create(schema, partitionSpec, tableProperties, changeLocation);
        } catch (Exception e) {
          throw new IllegalStateException("create change table failed", e);
        }
      });
      ChangeTable changeTable = new BaseKeyedTable.ChangeInternalTable(tableIdentifier,
          useArcticTableOperations(changeIcebergTable, changeLocation, fileIO, tableMetaStore.getConfiguration()),
          fileIO, client);
      return new BaseKeyedTable(meta, tableLocation,
          primaryKeySpec, client, baseTable, changeTable);
    }

    protected UnkeyedTable createUnKeyedTable(TableMeta meta) {
      TableIdentifier tableIdentifier = TableIdentifier.of(meta.getTableIdentifier());
      String baseLocation = checkLocation(meta, MetaTableProperties.LOCATION_KEY_BASE);

      Map<String, String> tableProperties = meta.getProperties();
      tableProperties.put(TableProperties.TABLE_CREATE_TIME, String.valueOf(System.currentTimeMillis()));
      Table table = tableMetaStore.doAs(() -> {
        try {
          return tables.create(schema, partitionSpec, meta.getProperties(), baseLocation);
        } catch (Exception e) {
          throw new IllegalStateException("create table failed", e);
        }
      });
      ArcticFileIO fileIO = new ArcticHadoopFileIO(tableMetaStore);
      return new BaseUnkeyedTable(tableIdentifier, useArcticTableOperations(table, baseLocation, fileIO,
          tableMetaStore.getConfiguration()), fileIO, client);
    }

    private String getTableLocationForCreate() {
      if (StringUtils.isNotBlank(location)) {
        return location;
      }

      String catalogWarehouseDir = null;
      if (catalogMeta.getCatalogProperties() != null) {
        catalogWarehouseDir = catalogMeta.getCatalogProperties().getOrDefault(
            CatalogMetaProperties.KEY_WAREHOUSE_DIR,
            null
        );
        if (!Objects.equals("/", catalogWarehouseDir) && catalogWarehouseDir.endsWith("/")) {
          catalogWarehouseDir = catalogWarehouseDir.substring(
              0, catalogWarehouseDir.length() - 1);
        }
      }
      String tableLocation = null;
      if (properties.containsKey(TableProperties.LOCATION)) {
        tableLocation = properties.get(TableProperties.LOCATION);
        if (tableLocation.endsWith("/")) {
          tableLocation = tableLocation.substring(
              0, tableLocation.length() - 1);
        }
      }
      if (StringUtils.isNotBlank(tableLocation)) {
        return tableLocation;
      } else if (StringUtils.isNotBlank(catalogWarehouseDir)) {
        return catalogWarehouseDir +
            '/' + identifier.getDatabase() +
            '/' + identifier.getTableName();
      } else {
        throw new IllegalStateException(
            "either `location` in table properties or " +
                "`warehouse.dir` in catalog properties is specified");
      }
    }

    protected TableMetadata tableMetadata(
        Schema schema, PartitionSpec spec, SortOrder order,
        Map<String, String> properties, String location) {
      Preconditions.checkNotNull(schema, "A table schema is required");

      Map<String, String> tableProps = properties == null ? ImmutableMap.of() : properties;
      PartitionSpec partitionSpec = spec == null ? PartitionSpec.unpartitioned() : spec;
      SortOrder sortOrder = order == null ? SortOrder.unsorted() : order;
      return TableMetadata.newTableMetadata(schema, partitionSpec, sortOrder, location, tableProps);
    }
  }
}
