package com.netease.arctic.hive;

import com.netease.arctic.table.TableMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.iceberg.hive.HiveClientPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extended implementation of {@link HiveClientPool} with {@link TableMetaStore} to support authenticated hive
 * cluster.
 */
public class ArcticHiveClientPool extends HiveClientPool {
  private final TableMetaStore metaStore;
  private static final Logger LOG = LoggerFactory.getLogger(ArcticHiveClientPool.class);

  public ArcticHiveClientPool(TableMetaStore tableMetaStore, int poolSize) {
    super(poolSize, tableMetaStore.getConfiguration());
    this.metaStore = tableMetaStore;
  }

  @Override
  protected HiveMetaStoreClient newClient() {
    return metaStore.doAs(() -> super.newClient());
  }

  @Override
  protected HiveMetaStoreClient reconnect(HiveMetaStoreClient client) {
    try {
      return metaStore.doAs(() -> super.reconnect(client));
    } catch (Exception e) {
      LOG.error("hive metastore client reconnected failed", e);
      throw e;
    }
  }
}