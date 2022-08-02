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

package com.netease.arctic.ams.server.service.impl;

import com.netease.arctic.AmsClient;
import com.netease.arctic.ams.api.Constants;
import com.netease.arctic.ams.api.DataFile;
import com.netease.arctic.ams.api.DataFileInfo;
import com.netease.arctic.ams.api.MetaException;
import com.netease.arctic.ams.api.PartitionFieldData;
import com.netease.arctic.ams.api.TableChange;
import com.netease.arctic.ams.api.TableCommitMeta;
import com.netease.arctic.ams.api.TableIdentifier;
import com.netease.arctic.ams.server.ArcticMetaStore;
import com.netease.arctic.ams.server.config.ArcticMetaStoreConf;
import com.netease.arctic.ams.server.mapper.FileInfoCacheMapper;
import com.netease.arctic.ams.server.mapper.SnapInfoCacheMapper;
import com.netease.arctic.ams.server.model.CacheFileInfo;
import com.netease.arctic.ams.server.model.CacheSnapshotInfo;
import com.netease.arctic.ams.server.model.PartitionBaseInfo;
import com.netease.arctic.ams.server.model.PartitionFileBaseInfo;
import com.netease.arctic.ams.server.model.SnapshotStatistics;
import com.netease.arctic.ams.server.model.TableMetadata;
import com.netease.arctic.ams.server.model.TransactionsOfTable;
import com.netease.arctic.ams.server.service.IJDBCService;
import com.netease.arctic.ams.server.service.IMetaService;
import com.netease.arctic.ams.server.service.ServiceContainer;
import com.netease.arctic.ams.server.utils.TableMetadataUtil;
import com.netease.arctic.ams.server.utils.ThreadPool;
import com.netease.arctic.catalog.ArcticCatalog;
import com.netease.arctic.catalog.CatalogLoader;
import com.netease.arctic.data.DataFileType;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.TableProperties;
import com.netease.arctic.utils.SnapshotFileUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ibatis.session.SqlSession;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class FileInfoCacheService extends IJDBCService {

  private static final Logger LOG = LoggerFactory.getLogger(FileInfoCacheService.class);

  public static ConcurrentHashMap<String, Long> cacheTableSnapshot = new ConcurrentHashMap<>();

  public void commitCacheFileInfo(TableCommitMeta tableCommitMeta) throws MetaException {
    if (needRepairCache(tableCommitMeta)) {
      LOG.warn("should not cache {}", tableCommitMeta);
      return;
    }

    List<CacheSnapshotInfo> cacheSnapInfoList = genSnapInfo(tableCommitMeta);
    List<CacheFileInfo> fileInfoList = genFileInfo(tableCommitMeta);

    try (SqlSession sqlSession = getSqlSession(false)) {
      FileInfoCacheMapper fileInfoCacheMapper = getMapper(sqlSession, FileInfoCacheMapper.class);
      fileInfoList.stream().filter(e -> e.getDeleteSnapshotId() == null)
          .forEach(fileInfoCacheMapper::insertCache);
      LOG.info("insert {} files into file cache", fileInfoList.stream().filter(e -> e.getDeleteSnapshotId() == null)
          .count());

      fileInfoList.stream().filter(e -> e.getDeleteSnapshotId() != null).forEach(fileInfoCacheMapper::updateCache);
      LOG.info("update {} files in file cache", fileInfoList.stream().filter(e -> e.getDeleteSnapshotId() != null)
          .count());

      SnapInfoCacheMapper snapInfoCacheMapper = getMapper(sqlSession, SnapInfoCacheMapper.class);
      cacheSnapInfoList.forEach(snapInfoCacheMapper::insertCache);
      LOG.info("insert {} snapshot into snapshot cache", cacheSnapInfoList.size());

      sqlSession.commit();
    } catch (Exception e) {
      LOG.error("insert file cache error", e);
    }
  }

  public List<DataFileInfo> getOptimizeDatafiles(TableIdentifier tableIdentifier, String tableType) {
    try (SqlSession sqlSession = getSqlSession(true)) {
      FileInfoCacheMapper fileInfoCacheMapper = getMapper(sqlSession, FileInfoCacheMapper.class);
      return fileInfoCacheMapper.getOptimizeDatafiles(tableIdentifier, tableType);
    }
  }

  public List<DataFileInfo> getChangeTableTTLDataFiles(TableIdentifier tableIdentifier, long ttl) {
    try (SqlSession sqlSession = getSqlSession(true)) {
      FileInfoCacheMapper fileInfoCacheMapper = getMapper(sqlSession, FileInfoCacheMapper.class);
      return fileInfoCacheMapper.getChangeTableTTLDataFiles(tableIdentifier, Constants.INNER_TABLE_CHANGE, ttl);
    }
  }

  public List<SnapshotStatistics> getCurrentSnapInfo(TableIdentifier identifier, String innerTable) {
    try (SqlSession sqlSession = getSqlSession(true)) {
      SnapInfoCacheMapper snapInfoCacheMapper = getMapper(sqlSession, SnapInfoCacheMapper.class);
      return snapInfoCacheMapper.getCurrentSnapInfo(identifier, innerTable);
    }
  }

  public Boolean snapshotIsCached(TableIdentifier identifier, String innerTable, Long snapshotId) {
    try (SqlSession sqlSession = getSqlSession(true)) {
      SnapInfoCacheMapper snapInfoCacheMapper = getMapper(sqlSession, SnapInfoCacheMapper.class);
      List<Long> snaps = snapInfoCacheMapper.getCurrentSnap(identifier, innerTable, snapshotId);

      return CollectionUtils.isNotEmpty(snaps);
    }
  }

  private void syncCache(TableIdentifier identifier, String innerTable, Long parentSnapId) {
    try (SqlSession sqlSession = getSqlSession(true)) {

      SnapInfoCacheMapper snapInfoCacheMapper = getMapper(sqlSession, SnapInfoCacheMapper.class);
      List<Long> snaps = snapInfoCacheMapper.getCurrentSnap(identifier, innerTable, parentSnapId);

      if (CollectionUtils.isNotEmpty(snaps)) {
        Long currSnap = snaps.get(0);
        String innerTableIdentifier =
            TableMetadataUtil.getTableAllIdentifyName(identifier) + "." + innerTable;
        cacheTableSnapshot.put(innerTableIdentifier, currSnap);
      }
    }
  }

  /**
   * @param time delete all cache which commit time less than time and is deleted
   */
  public void expiredCache(long time, com.netease.arctic.table.TableIdentifier identifier) {
    try (SqlSession sqlSession = getSqlSession(true)) {
      FileInfoCacheMapper fileInfoCacheMapper = getMapper(sqlSession, FileInfoCacheMapper.class);
      fileInfoCacheMapper.expireCache(time);

      SnapInfoCacheMapper snapInfoCacheMapper = getMapper(sqlSession, SnapInfoCacheMapper.class);
      snapInfoCacheMapper.expireCache(time);
    }
  }

  public void syncTableFileInfo(TableIdentifier identifier, String tableType, Long from, Long to) {
    LOG.info("start sync table {} file info", identifier);
    try {
      // load table
      Table table = null;
      try {
        AmsClient client = ServiceContainer.getTableMetastoreHandler();
        ArcticCatalog catalog = CatalogLoader.load(client, identifier.getCatalog());
        com.netease.arctic.table.TableIdentifier tmp = com.netease.arctic.table.TableIdentifier.of(
            identifier.getCatalog(),
            identifier.getDatabase(),
            identifier.getTableName());
        ArcticTable arcticTable = catalog.loadTable(tmp);
        if (arcticTable.isUnkeyedTable()) {
          table = arcticTable.asUnkeyedTable();
        } else {
          if (Constants.INNER_TABLE_CHANGE.equalsIgnoreCase(tableType)) {
            table = arcticTable.asKeyedTable().changeTable();
          } else {
            table = arcticTable.asKeyedTable().baseTable();
          }
        }
      } catch (Exception e) {
        LOG.warn("load table error when sync file info cache:" + identifier.getCatalog() + identifier.getDatabase() +
            identifier.getTableName(), e);
      }

      // get snapshot info
      if (table == null) {
        return;
      }
      if (table.currentSnapshot() == null) {
        return;
      }
      long currId = table.currentSnapshot().snapshotId();
      if (currId == -1) {
        return;
      }
      long fromId = from == null ? -1 : from;
      long toId = to == null ? currId : to;
      // if there is no new snapshots commit in table after last sync will not sync file cache
      if (fromId == toId) {
        return;
      }
      List<Snapshot> snapshots = snapshotsWithin(table, fromId, toId);

      // generate cache info
      LOG.info("{} start sync file info", identifier);
      ArcticTable finalTable = (ArcticTable) table;
      finalTable.io().doAs(() -> {
        syncFileInfo(finalTable, identifier, tableType, snapshots);
        return null;
      });

      // update local on-memory cache
      cacheTableSnapshot.put(TableMetadataUtil.getTableAllIdentifyName(identifier) + tableType, toId);
    } catch (Exception e) {
      LOG.error("sync cache info error " + identifier, e);
    }
  }

  public void deleteTableCache(com.netease.arctic.table.TableIdentifier identifier) {
    TableIdentifier tableIdentifier = new TableIdentifier();
    tableIdentifier.catalog = identifier.getCatalog();
    tableIdentifier.database = identifier.getDatabase();
    tableIdentifier.tableName = identifier.getTableName();
    try (SqlSession sqlSession = getSqlSession(true)) {
      FileInfoCacheMapper fileInfoCacheMapper = getMapper(sqlSession, FileInfoCacheMapper.class);
      fileInfoCacheMapper.deleteTableCache(tableIdentifier);

      SnapInfoCacheMapper snapInfoCacheMapper = getMapper(sqlSession, SnapInfoCacheMapper.class);
      snapInfoCacheMapper.deleteTableCache(tableIdentifier);

      // update local on-memory cache
      cacheTableSnapshot
          .remove(TableMetadataUtil.getTableAllIdentifyName(tableIdentifier) + Constants.INNER_TABLE_BASE);
      cacheTableSnapshot
          .remove(TableMetadataUtil.getTableAllIdentifyName(tableIdentifier) + Constants.INNER_TABLE_CHANGE);
    } catch (Exception e) {
      LOG.error("delete table file cache error ", e);
    }
  }

  private boolean needRepairCache(TableCommitMeta tableCommitMeta) {
    if (CollectionUtils.isNotEmpty(tableCommitMeta.getChanges())) {
      TableChange tableChange = tableCommitMeta.getChanges().get(0);
      String innerTableIdentifier =
          TableMetadataUtil.getTableAllIdentifyName(tableCommitMeta.getTableIdentifier()) + "." +
              tableChange.getInnerTable();

      // check whether cache snapshot id is continuous
      Long commitParent = tableChange.getParentSnapshotId();
      Long cacheParent = cacheTableSnapshot.get(innerTableIdentifier);
      if (commitParent == -1) {
        return false;
      }
      if (!commitParent.equals(cacheParent)) {
        syncCache(tableCommitMeta.getTableIdentifier(), tableChange.getInnerTable(), commitParent);
        cacheParent = cacheTableSnapshot.get(innerTableIdentifier);
      }
      if (!commitParent.equals(cacheParent)) {
        final Long finalCacheParent = cacheParent;
        ThreadPool.getSyncFileInfoCachePool().execute(() ->
            syncTableFileInfo(tableCommitMeta.getTableIdentifier(), tableChange.getInnerTable(), finalCacheParent,
                tableChange.getSnapshotId())
        );
        return true;
      }
    }
    return false;
  }

  private static List<Snapshot> snapshotsWithin(Table table, long fromSnapshotId, long toSnapshotId) {
    List<Long> snapshotIds = SnapshotUtil.snapshotIdsBetween(table, fromSnapshotId, toSnapshotId);
    List<Snapshot> snapshots = Lists.newArrayList();
    snapshotIds.forEach(id -> snapshots.add(table.snapshot(id)));
    return snapshots;
  }

  private void syncFileInfo(
      ArcticTable table,
      TableIdentifier identifier,
      String tableType,
      List<Snapshot> snapshots) {
    for (int i = snapshots.size() - 1; i >= 0; i--) {
      List<CacheFileInfo> fileInfos = new ArrayList<>();
      List<DataFile> addFiles = new ArrayList<>();
      List<DataFile> deleteFiles = new ArrayList<>();
      Snapshot snapshot = snapshots.get(i);
      SnapshotFileUtil.getSnapshotFiles(table, snapshot, addFiles, deleteFiles);
      for (DataFile amsFile : addFiles) {
        String partitionName = StringUtils.isEmpty(partitionToPath(amsFile.getPartition())) ?
            null :
            partitionToPath(amsFile.getPartition());
        long watermark = 0L;
        boolean isDataFile = Objects.equals(amsFile.fileType, DataFileType.INSERT_FILE.name()) ||
            Objects.equals(amsFile.fileType, DataFileType.BASE_FILE.name());
        if (isDataFile &&
            table.properties() != null && table.properties().containsKey(TableProperties.TABLE_EVENT_TIME_FIELD)) {
          watermark =
              amsFile.getUpperBounds()
                  .get(table.properties().get(TableProperties.TABLE_EVENT_TIME_FIELD))
                  .getLong();
        }
        CacheFileInfo cacheFileInfo = new CacheFileInfo(identifier, snapshot.snapshotId(), null, tableType,
            amsFile.getPath(), amsFile.getFileType(), amsFile.getFileSize(), amsFile.getMask(),
            amsFile.getIndex(), amsFile.getSpecId(), partitionName, snapshot.timestampMillis(),
            amsFile.getRecordCount(), snapshot.operation(), watermark);

        fileInfos.add(cacheFileInfo);
      }

      for (DataFile amsFile : deleteFiles) {
        CacheFileInfo cacheFileInfo = new CacheFileInfo();
        cacheFileInfo.setTableIdentifier(identifier);
        cacheFileInfo.setDeleteSnapshotId(snapshot.snapshotId());
        cacheFileInfo.setFilePath(amsFile.getPath());
        cacheFileInfo.setInnerTable(tableType);
        fileInfos.add(cacheFileInfo);
      }
      CacheSnapshotInfo snapshotInfo = syncSnapInfo(identifier, tableType, snapshot);

      try (SqlSession sqlSession = getSqlSession(false)) {
        FileInfoCacheMapper fileInfoCacheMapper = getMapper(sqlSession, FileInfoCacheMapper.class);
        fileInfos.stream().filter(e -> e.getDeleteSnapshotId() == null).forEach(fileInfoCacheMapper::insertCache);
        fileInfos.stream().filter(e -> e.getDeleteSnapshotId() != null).forEach(fileInfoCacheMapper::updateCache);

        SnapInfoCacheMapper snapInfoCacheMapper = getMapper(sqlSession, SnapInfoCacheMapper.class);
        snapInfoCacheMapper.insertCache(snapshotInfo);

        sqlSession.commit();
      } catch (Exception e) {
        LOG.error("insert file cache error", e);
      }
    }
  }

  private List<CacheFileInfo> genFileInfo(TableCommitMeta tableCommitMeta) {
    List<CacheFileInfo> rs = new ArrayList<>();

    if (CollectionUtils.isNotEmpty(tableCommitMeta.getChanges())) {
      tableCommitMeta.getChanges().forEach(tableChange -> {
        if (CollectionUtils.isNotEmpty(tableChange.getAddFiles())) {
          tableChange.getAddFiles().forEach(datafile -> {
            CacheFileInfo cacheFileInfo = new CacheFileInfo();
            cacheFileInfo.setTableIdentifier(tableCommitMeta.getTableIdentifier());
            cacheFileInfo.setAddSnapshotId(tableChange.getSnapshotId());
            cacheFileInfo.setInnerTable(tableChange.getInnerTable());
            cacheFileInfo.setFilePath(datafile.getPath());
            cacheFileInfo.setFileType(datafile.getFileType());
            cacheFileInfo.setFileSize(datafile.getFileSize());
            cacheFileInfo.setFileMask(datafile.getMask());
            cacheFileInfo.setFileIndex(datafile.getIndex());
            cacheFileInfo.setRecordCount(datafile.getRecordCount());
            cacheFileInfo.setSpecId(datafile.getSpecId());
            if (tableCommitMeta.getProperties() != null &&
                tableCommitMeta.getProperties().containsKey(TableProperties.TABLE_EVENT_TIME_FIELD)) {
              Long watermark =
                  datafile.getUpperBounds()
                      .get(tableCommitMeta.getProperties().get(TableProperties.TABLE_EVENT_TIME_FIELD))
                      .getLong();
              cacheFileInfo.setWatermark(watermark);
            } else {
              cacheFileInfo.setWatermark(0L);
            }
            cacheFileInfo.setAction(tableCommitMeta.getAction());
            String partitionName = partitionToPath(datafile.getPartition());
            cacheFileInfo.setPartitionName(StringUtils.isEmpty(partitionName) ? null : partitionName);
            cacheFileInfo.setCommitTime(tableCommitMeta.getCommitTime());
            rs.add(cacheFileInfo);
          });
        }

        if (CollectionUtils.isNotEmpty(tableChange.getDeleteFiles())) {
          tableChange.getDeleteFiles().forEach(datafile -> {
            CacheFileInfo cacheFileInfo = new CacheFileInfo();
            cacheFileInfo.setTableIdentifier(tableCommitMeta.getTableIdentifier());
            cacheFileInfo.setDeleteSnapshotId(tableChange.getSnapshotId());
            cacheFileInfo.setFilePath(datafile.getPath());
            cacheFileInfo.setInnerTable(tableChange.innerTable);
            rs.add(cacheFileInfo);
          });
        }
      });
    }
    return rs;
  }

  private CacheSnapshotInfo syncSnapInfo(TableIdentifier identifier, String tableType, Snapshot snapshot) {
    CacheSnapshotInfo cache = new CacheSnapshotInfo();
    cache.setTableIdentifier(identifier);
    cache.setSnapshotId(snapshot.snapshotId());
    cache.setParentSnapshotId(snapshot.parentId() == null ? -1 : snapshot.parentId());
    cache.setAction(snapshot.operation());
    cache.setInnerTable(tableType);
    cache.setCommitTime(snapshot.timestampMillis());
    return cache;
  }

  private List<CacheSnapshotInfo> genSnapInfo(TableCommitMeta tableCommitMeta) {
    List<CacheSnapshotInfo> rs = new ArrayList<>();
    if (CollectionUtils.isNotEmpty(tableCommitMeta.getChanges())) {
      tableCommitMeta.getChanges().forEach(tableChange -> {
        String innerTableIdentifier =
            TableMetadataUtil.getTableAllIdentifyName(tableCommitMeta.getTableIdentifier()) + "." +
                tableChange.getInnerTable();

        CacheSnapshotInfo cache = new CacheSnapshotInfo();
        cache.setTableIdentifier(tableCommitMeta.getTableIdentifier());
        cache.setSnapshotId(tableChange.getSnapshotId());
        cache.setParentSnapshotId(tableChange.getParentSnapshotId());
        cache.setAction(tableCommitMeta.getAction());
        cache.setInnerTable(tableChange.getInnerTable());
        cache.setCommitTime(tableCommitMeta.getCommitTime());
        rs.add(cache);

        cacheTableSnapshot.put(innerTableIdentifier, tableChange.getSnapshotId());
      });
    }
    return rs;
  }

  public List<TransactionsOfTable> getTransactions(TableIdentifier tableIdentifier) {
    try (SqlSession sqlSession = getSqlSession(true)) {
      FileInfoCacheMapper fileInfoCacheMapper = getMapper(sqlSession, FileInfoCacheMapper.class);
      return fileInfoCacheMapper.getTransactions(tableIdentifier);
    }
  }

  public List<DataFileInfo> getDatafilesInfo(TableIdentifier tableIdentifier, Long transactionId) {
    try (SqlSession sqlSession = getSqlSession(true)) {
      FileInfoCacheMapper fileInfoCacheMapper = getMapper(sqlSession, FileInfoCacheMapper.class);
      return fileInfoCacheMapper.getDatafilesInfo(tableIdentifier, transactionId);
    }
  }

  public List<PartitionBaseInfo> getPartitionBaseInfoList(TableIdentifier tableIdentifier) {
    try (SqlSession sqlSession = getSqlSession(true)) {
      FileInfoCacheMapper fileInfoCacheMapper = getMapper(sqlSession, FileInfoCacheMapper.class);
      return fileInfoCacheMapper.getPartitionBaseInfoList(tableIdentifier);
    }
  }

  public List<PartitionFileBaseInfo> getPartitionFileList(TableIdentifier tableIdentifier, String partition) {
    try (SqlSession sqlSession = getSqlSession(true)) {
      FileInfoCacheMapper fileInfoCacheMapper = getMapper(sqlSession, FileInfoCacheMapper.class);
      return fileInfoCacheMapper.getPartitionFileList(tableIdentifier, partition);
    }
  }

  public Long getWatermark(TableIdentifier tableIdentifier, String innerTable) {
    try (SqlSession sqlSession = getSqlSession(true)) {
      FileInfoCacheMapper fileInfoCacheMapper = getMapper(sqlSession, FileInfoCacheMapper.class);
      Timestamp watermark = fileInfoCacheMapper.getWatermark(tableIdentifier, innerTable);
      return watermark == null ? 0 : watermark.getTime();
    }
  }

  private String partitionToPath(List<PartitionFieldData> partitionFieldDataList) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < partitionFieldDataList.size(); i++) {
      if (i > 0) {
        sb.append("/");
      }
      sb.append(partitionFieldDataList.get(i).getName()).append("=")
          .append(partitionFieldDataList.get(i).getValue());
    }
    return sb.toString();
  }

  public static class SyncAndExpireFileCacheTask {

    public static final Logger LOG = LoggerFactory.getLogger(SyncAndExpireFileCacheTask.class);

    private final FileInfoCacheService fileInfoCacheService;
    private final IMetaService metaService;

    public SyncAndExpireFileCacheTask() {
      this.fileInfoCacheService = ServiceContainer.getFileInfoCacheService();
      this.metaService = ServiceContainer.getMetaService();
    }

    public void doTask() {
      LOG.info("start execute doTask");
      syncCache();
    }

    private void syncCache() {
      LOG.info("start execute syncCache");
      List<TableMetadata> tableMetadata = metaService.listTables();
      long lowTime = System.currentTimeMillis() -
          ArcticMetaStore.conf.getLong(ArcticMetaStoreConf.TABLE_FILE_INFO_CACHE_INTERVAL);
      tableMetadata.forEach(meta -> {
        if (meta.getTableIdentifier() == null) {
          return;
        }
        TableIdentifier tableIdentifier = new TableIdentifier();
        tableIdentifier.catalog = meta.getTableIdentifier().getCatalog();
        tableIdentifier.database = meta.getTableIdentifier().getDatabase();
        tableIdentifier.tableName = meta.getTableIdentifier().getTableName();
        try {
          ArcticCatalog catalog =
              CatalogLoader.load(ServiceContainer.getTableMetastoreHandler(), tableIdentifier.getCatalog());
          com.netease.arctic.table.TableIdentifier tmp = com.netease.arctic.table.TableIdentifier.of(
              tableIdentifier.getCatalog(),
              tableIdentifier.getDatabase(),
              tableIdentifier.getTableName());
          ArcticTable arcticTable = catalog.loadTable(tmp);
          doSync(tableIdentifier, Constants.INNER_TABLE_BASE, lowTime);
          if (arcticTable.isKeyedTable()) {
            doSync(tableIdentifier, Constants.INNER_TABLE_CHANGE, lowTime);
          }
        } catch (Exception e) {
          LOG.error(
              "SyncAndExpireFileCacheTask sync cache error " + tableIdentifier.catalog + tableIdentifier.database +
                  tableIdentifier.tableName, e);
        }
      });
    }

    private void doSync(TableIdentifier tableIdentifier, String innerTable, long lowTime) {
      try {
        List<SnapshotStatistics> snapshotStatistics =
            fileInfoCacheService.getCurrentSnapInfo(tableIdentifier, innerTable);
        if (CollectionUtils.isEmpty(snapshotStatistics)) {
          fileInfoCacheService.syncTableFileInfo(tableIdentifier, innerTable, null, null);
        } else {
          if (snapshotStatistics.get(0).getCommitTime() < lowTime) {
            fileInfoCacheService.syncTableFileInfo(tableIdentifier, innerTable, snapshotStatistics.get(0).getId(),
                null);
          }
        }
      } catch (Exception e) {
        LOG.error("period sync file cache error", e);
      }
    }
  }
}
