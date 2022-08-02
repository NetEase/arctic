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

package com.netease.arctic.ams.server.optimize;

import com.netease.arctic.ams.api.OptimizeType;
import com.netease.arctic.ams.server.model.BaseOptimizeTask;
import com.netease.arctic.ams.server.model.TableOptimizeRuntime;
import com.netease.arctic.ams.server.model.TableTaskHistory;
import com.netease.arctic.data.DataTreeNode;
import com.netease.arctic.data.DefaultKeyedFile;
import com.netease.arctic.op.OverwriteBaseFiles;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.UnkeyedTable;
import com.netease.arctic.utils.SerializationUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.util.StructLikeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class BaseOptimizeCommit {
  private static final Logger LOG = LoggerFactory.getLogger(BaseOptimizeCommit.class);
  private final ArcticTable arcticTable;
  private final Map<String, List<OptimizeTaskItem>> optimizeTasksToCommit;
  private final Map<String, TableTaskHistory> commitTableTaskHistory = new HashMap<>();
  private final Map<String, OptimizeType> partitionOptimizeType = new HashMap<>();

  public BaseOptimizeCommit(ArcticTable arcticTable, Map<String, List<OptimizeTaskItem>> optimizeTasksToCommit) {
    this.arcticTable = arcticTable;
    this.optimizeTasksToCommit = optimizeTasksToCommit;
  }

  public Map<String, List<OptimizeTaskItem>> getCommittedTasks() {
    return optimizeTasksToCommit;
  }

  public Map<String, TableTaskHistory> getCommitTableTaskHistory() {
    return commitTableTaskHistory;
  }

  public long commit(TableOptimizeRuntime tableOptimizeRuntime) throws Exception {
    try {
      if (optimizeTasksToCommit.isEmpty()) {
        LOG.info("{} get no tasks to commit", arcticTable.id());
        return 0;
      }
      LOG.info("{} get tasks to commit for partitions {}", arcticTable.id(),
          optimizeTasksToCommit.keySet());

      // collect files
      Set<ContentFile<?>> minorAddFiles = new HashSet<>();
      Set<ContentFile<?>> minorDeleteFiles = new HashSet<>();
      Set<ContentFile<?>> majorAddFiles = new HashSet<>();
      Set<ContentFile<?>> majorDeleteFiles = new HashSet<>();
      PartitionSpec spec = arcticTable.spec();
      StructLikeMap<Long> maxTransactionIds = StructLikeMap.create(spec.partitionType());
      for (Map.Entry<String, List<OptimizeTaskItem>> entry : optimizeTasksToCommit.entrySet()) {
        for (OptimizeTaskItem task : entry.getValue()) {
          // tasks in partition
          if (task.getOptimizeTask().getTaskId().getType() == OptimizeType.Minor) {
            task.getOptimizeRuntime().getTargetFiles().stream()
                .map(SerializationUtil::toInternalTableFile)
                .forEach(minorAddFiles::add);

            minorDeleteFiles.addAll(selectDeletedFiles(task.getOptimizeTask(), minorAddFiles));

            long maxTransactionId = task.getOptimizeTask().getMaxChangeTransactionId();
            if (maxTransactionId != BaseOptimizeTask.INVALID_TRANSACTION_ID) {
              if (arcticTable.asKeyedTable().baseTable().spec().isUnpartitioned()) {
                maxTransactionIds.put(null, maxTransactionId);
              } else {
                maxTransactionIds.putIfAbsent(
                    DataFiles.data(spec, entry.getKey()), maxTransactionId);
              }
            }
            partitionOptimizeType.put(entry.getKey(), OptimizeType.Minor);
          } else {
            task.getOptimizeRuntime().getTargetFiles().stream()
                .map(SerializationUtil::toInternalTableFile)
                .forEach(majorAddFiles::add);
            majorDeleteFiles.addAll(selectDeletedFiles(task.getOptimizeTask(), new HashSet<>()));
            partitionOptimizeType.put(entry.getKey(), OptimizeType.Major);
          }

          String taskGroupId = task.getOptimizeTask().getTaskGroup();
          String taskHistoryId = task.getOptimizeTask().getTaskHistoryId();
          String historyKey = taskHistoryId + "#" + taskGroupId;
          TableTaskHistory tableTaskHistory = commitTableTaskHistory.get(historyKey);
          if (tableTaskHistory != null) {
            tableTaskHistory.setCostTime(tableTaskHistory.getCostTime() + task.getOptimizeRuntime().getCostTime());
            tableTaskHistory.setStartTime(Math.min(tableTaskHistory.getStartTime(),
                task.getOptimizeRuntime().getExecuteTime()));
            tableTaskHistory.setEndTime(Math.max(tableTaskHistory.getEndTime(),
                task.getOptimizeRuntime().getReportTime()));
          } else {
            tableTaskHistory = new TableTaskHistory();
            tableTaskHistory.setTableIdentifier(arcticTable.id());
            tableTaskHistory.setTaskGroupId(taskGroupId);
            tableTaskHistory.setTaskHistoryId(taskHistoryId);
            tableTaskHistory.setCostTime(task.getOptimizeRuntime().getCostTime());
            tableTaskHistory.setStartTime(task.getOptimizeRuntime().getExecuteTime());
            tableTaskHistory.setEndTime(task.getOptimizeRuntime().getReportTime());
          }
          commitTableTaskHistory.put(historyKey, tableTaskHistory);
        }
      }

      UnkeyedTable baseArcticTable;
      if (arcticTable.isKeyedTable()) {
        baseArcticTable = arcticTable.asKeyedTable().baseTable();
      } else {
        baseArcticTable = arcticTable.asUnkeyedTable();
      }

      // commit minor optimize content
      if (CollectionUtils.isNotEmpty(minorAddFiles) || CollectionUtils.isNotEmpty(minorDeleteFiles)) {
        OverwriteBaseFiles overwriteBaseFiles = new OverwriteBaseFiles(arcticTable.asKeyedTable());
        AtomicInteger addedPosDeleteFile = new AtomicInteger(0);
        minorAddFiles.forEach(contentFile -> {
          if (contentFile instanceof DataFile) {
            overwriteBaseFiles.addFile((DataFile) contentFile);
          } else {
            overwriteBaseFiles.addFile((DeleteFile) contentFile);
            addedPosDeleteFile.incrementAndGet();
          }
        });
        AtomicInteger deletedPosDeleteFile = new AtomicInteger(0);
        Set<DeleteFile> deletedPosDeleteFiles = new HashSet<>();
        minorDeleteFiles.forEach(contentFile -> {
          if (contentFile instanceof DataFile) {
            overwriteBaseFiles.deleteFile((DataFile) contentFile);
          } else {
            deletedPosDeleteFiles.add((DeleteFile) contentFile);
          }
        });

        if (spec.isUnpartitioned()) {
          overwriteBaseFiles.withMaxTransactionId(null, maxTransactionIds.get(null));
        } else {
          maxTransactionIds.forEach(overwriteBaseFiles::withMaxTransactionId);
        }
        overwriteBaseFiles.commit();

        if (CollectionUtils.isNotEmpty(deletedPosDeleteFiles)) {
          RewriteFiles rewriteFiles = baseArcticTable.newRewrite();
          rewriteFiles.rewriteFiles(Collections.emptySet(), deletedPosDeleteFiles,
              Collections.emptySet(), Collections.emptySet());
          try {
            rewriteFiles.commit();
          } catch (ValidationException e) {
            LOG.warn("Iceberg RewriteFiles commit failed, but ignore", e);
          }
        }

        LOG.info("{} minor optimize committed, delete {} files [{} posDelete files], " +
                "add {} new files [{} posDelete files]",
            arcticTable.id(), minorDeleteFiles.size(), deletedPosDeleteFile.get(), minorAddFiles.size(),
            addedPosDeleteFile.get());
      } else {
        LOG.info("{} skip minor optimize commit", arcticTable.id());
      }

      // commit major optimize content
      if (CollectionUtils.isNotEmpty(majorAddFiles) || CollectionUtils.isNotEmpty(majorDeleteFiles)) {
        Set<DataFile> addDataFiles = majorAddFiles.stream().map(contentFile -> {
          if (contentFile instanceof DataFile) {
            return (DataFile) contentFile;
          }

          return null;
        }).filter(Objects::nonNull).collect(Collectors.toSet());
        Set<DeleteFile> addDeleteFiles = majorAddFiles.stream().map(contentFile -> {
          if (contentFile instanceof DeleteFile) {
            return (DeleteFile) contentFile;
          }

          return null;
        }).filter(Objects::nonNull).collect(Collectors.toSet());

        Set<DataFile> deleteDataFiles = majorDeleteFiles.stream().map(contentFile -> {
          if (contentFile instanceof DataFile) {
            return (DataFile) contentFile;
          }

          return null;
        }).filter(Objects::nonNull).collect(Collectors.toSet());
        Set<DeleteFile> deleteDeleteFiles = majorDeleteFiles.stream().map(contentFile -> {
          if (contentFile instanceof DeleteFile) {
            return (DeleteFile) contentFile;
          }

          return null;
        }).filter(Objects::nonNull).collect(Collectors.toSet());

        if (!addDeleteFiles.isEmpty()) {
          throw new IllegalArgumentException("for major optimize, can't add delete files " + addDeleteFiles);
        }
        if (deleteDeleteFiles.isEmpty()) {
          OverwriteFiles overwriteFiles = baseArcticTable.newOverwrite();
          deleteDataFiles.forEach(overwriteFiles::deleteFile);
          addDataFiles.forEach(overwriteFiles::addFile);
          overwriteFiles.commit();
        } else {
          RewriteFiles rewriteFiles = baseArcticTable.newRewrite()
              .validateFromSnapshot(baseArcticTable.currentSnapshot().snapshotId());
          rewriteFiles.rewriteFiles(deleteDataFiles, deleteDeleteFiles, addDataFiles, addDeleteFiles);
          rewriteFiles.commit();
        }
        
        LOG.info("{} major optimize committed, delete {} files [{} posDelete files], " +
                "add {} new files [{} posDelete files]",
            arcticTable.id(), majorDeleteFiles.size(), deleteDeleteFiles.size(), majorAddFiles.size(),
            addDeleteFiles.size());

        // update table runtime base snapshotId, avoid repeat Major plan
        if (arcticTable.isKeyedTable()) {
          tableOptimizeRuntime.setCurrentSnapshotId(arcticTable.asKeyedTable().baseTable()
              .currentSnapshot().snapshotId());
        } else {
          tableOptimizeRuntime.setCurrentSnapshotId(arcticTable.asUnkeyedTable()
              .currentSnapshot().snapshotId());
        }
      } else {
        LOG.info("{} skip major optimize commit", arcticTable.id());
      }
      return System.currentTimeMillis();
    } catch (Throwable t) {
      LOG.error("unexpected commit error " + arcticTable.id(), t);
      throw new Exception("unexpected commit error ", t);
    }
  }

  public Map<String, OptimizeType> getPartitionOptimizeType() {
    return partitionOptimizeType;
  }

  private static Set<ContentFile<?>> selectDeletedFiles(BaseOptimizeTask optimizeTask,
                                                        Set<ContentFile<?>> addPosDeleteFiles) {
    switch (optimizeTask.getTaskId().getType()) {
      case Major:
        return selectMajorOptimizeDeletedFiles(optimizeTask);
      case Minor:
        return selectMinorOptimizeDeletedFiles(optimizeTask, addPosDeleteFiles);
    }

    return new HashSet<>();
  }

  private static Set<ContentFile<?>> selectMinorOptimizeDeletedFiles(BaseOptimizeTask optimizeTask,
                                                                     Set<ContentFile<?>> addPosDeleteFiles) {
    Set<DataTreeNode> newFileNodes = addPosDeleteFiles.stream().map(contentFile -> {
      if (contentFile instanceof DeleteFile) {
        return DefaultKeyedFile.parseMetaFromFileName(contentFile.path().toString()).node();
      }

      return null;
    }).filter(Objects::nonNull).collect(Collectors.toSet());

    return optimizeTask.getPosDeleteFiles().stream().map(SerializationUtil::toInternalTableFile)
        .filter(posDeleteFile ->
            newFileNodes.contains(DefaultKeyedFile.parseMetaFromFileName(posDeleteFile.path().toString()).node()))
        .collect(Collectors.toSet());
  }

  private static Set<ContentFile<?>> selectMajorOptimizeDeletedFiles(BaseOptimizeTask optimizeTask) {
    // add base deleted files
    Set<ContentFile<?>> result = optimizeTask.getBaseFiles().stream()
        .map(SerializationUtil::toInternalTableFile).collect(Collectors.toSet());

    if (optimizeTask.getIsDeletePosDelete() == 1) {
      result.addAll(optimizeTask.getPosDeleteFiles().stream()
          .map(SerializationUtil::toInternalTableFile).collect(Collectors.toSet()));
    }

    return result;
  }
}
