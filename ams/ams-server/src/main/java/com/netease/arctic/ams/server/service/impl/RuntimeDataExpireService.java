package com.netease.arctic.ams.server.service.impl;

import com.netease.arctic.ams.server.model.TableMetadata;
import com.netease.arctic.ams.server.service.IMetaService;
import com.netease.arctic.ams.server.service.ServiceContainer;
import com.netease.arctic.table.TableIdentifier;
import java.util.List;

public class RuntimeDataExpireService {
  private final ArcticTransactionService transactionService;
  private final IMetaService metaService;

  Long txDataExpireInterval = 24 * 60 * 60 * 1000L;

  public RuntimeDataExpireService() {
    this.transactionService = ServiceContainer.getArcticTransactionService();
    this.metaService = ServiceContainer.getMetaService();
  }

  public void doExpire() {
    List<TableMetadata> tableMetadata = metaService.listTables();
    tableMetadata.forEach(meta -> {
      TableIdentifier identifier = meta.getTableIdentifier();
      transactionService.expire(
          identifier.buildTableIdentifier(),
          System.currentTimeMillis() - this.txDataExpireInterval);
    });
  }
}
