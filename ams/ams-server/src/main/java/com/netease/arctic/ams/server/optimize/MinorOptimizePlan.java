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

import com.google.common.collect.ImmutableList;
import com.netease.arctic.ams.api.DataFileInfo;
import com.netease.arctic.ams.api.OptimizeType;
import com.netease.arctic.ams.server.model.BaseOptimizeTask;
import com.netease.arctic.ams.server.model.FileTree;
import com.netease.arctic.ams.server.model.TableOptimizeRuntime;
import com.netease.arctic.ams.server.model.TaskConfig;
import com.netease.arctic.ams.server.utils.ContentFileUtil;
import com.netease.arctic.data.DataFileType;
import com.netease.arctic.data.DataTreeNode;
import com.netease.arctic.data.DefaultKeyedFile;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.KeyedTable;
import com.netease.arctic.table.TableProperties;
import org.apache.commons.collections.CollectionUtils;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class MinorOptimizePlan extends BaseOptimizePlan {
  private static final Logger LOG = LoggerFactory.getLogger(MinorOptimizePlan.class);

  // partition -> delete file
  protected final Map<String, List<DataFile>> partitionDeleteFiles = new LinkedHashMap<>();
  // partition -> maxBaseTableTransactionId
  protected Map<String, Long> baseTableMaxTransactionId = null;
  // partition -> maxChangeTableTransactionId
  protected final Map<String, Long> changeTableMaxTransactionId = new HashMap<>();

  public MinorOptimizePlan(ArcticTable arcticTable, TableOptimizeRuntime tableOptimizeRuntime,
                           List<DataFileInfo> baseTableFileList,
                           List<DataFileInfo> changeTableFileList,
                           List<DataFileInfo> posDeleteFileList,
                           Map<String, Boolean> partitionTaskRunning,
                           int queueId, long currentTime, Predicate<Long> snapshotIsCached) {
    super(arcticTable, tableOptimizeRuntime, baseTableFileList, changeTableFileList, posDeleteFileList,
        partitionTaskRunning, queueId, currentTime, snapshotIsCached);
  }

  @Override
  public boolean partitionNeedPlan(String partitionToPath) {
    long current = System.currentTimeMillis();

    // check delete file count
    List<DataFile> deleteFileList = partitionDeleteFiles.get(partitionToPath);
    if (CollectionUtils.isNotEmpty(deleteFileList)) {
      // file count
      if (deleteFileList.size() >= PropertyUtil.propertyAsInt(arcticTable.properties(),
          TableProperties.MINOR_OPTIMIZE_TRIGGER_DELETE_FILE_COUNT,
          TableProperties.MINOR_OPTIMIZE_TRIGGER_DELETE_FILE_COUNT_DEFAULT)) {
        return true;
      }
    }

    // optimize interval
    if (current - tableOptimizeRuntime.getLatestMinorOptimizeTime(partitionToPath) >=
        PropertyUtil.propertyAsLong(arcticTable.properties(), TableProperties.MINOR_OPTIMIZE_TRIGGER_MAX_INTERVAL,
            TableProperties.MINOR_OPTIMIZE_TRIGGER_MAX_INTERVAL_DEFAULT)) {
      return true;
    }
    LOG.debug("{} ==== don't need {} optimize plan, skip partition {}", tableId(), getOptimizeType(), partitionToPath);
    return false;
  }

  @Override
  public void addOptimizeFilesTree() {
    addChangeFilesIntoFileTree();
    addBaseFileIntoFileTree();
  }

  @Override
  protected OptimizeType getOptimizeType() {
    return OptimizeType.Minor;
  }

  @Override
  protected List<BaseOptimizeTask> collectTask(String partition) {
    List<BaseOptimizeTask> result;

    FileTree treeRoot = partitionFileTree.get(partition);
    result = collectKeyedTableTasks(partition, treeRoot);
    // init files
    partitionDeleteFiles.put(partition, Collections.emptyList());
    partitionPosDeleteFiles.put(partition, Collections.emptyList());
    partitionFileTree.get(partition).initFiles();

    return result;
  }

  private void addChangeFilesIntoFileTree() {
    LOG.debug("{} start {} plan change files", tableId(), getOptimizeType());
    KeyedTable keyedArcticTable = arcticTable.asKeyedTable();

    AtomicInteger addCnt = new AtomicInteger();
    List<DataFile> changeOptimizeFiles = changeTableFileList.stream().map(dataFileInfo -> {
      PartitionSpec partitionSpec = keyedArcticTable.changeTable()
          .specs().get((int) dataFileInfo.getSpecId());
      String partition = dataFileInfo.getPartition() == null ? "" : dataFileInfo.getPartition();

      if (partitionSpec == null) {
        LOG.error("{} {} can not find partitionSpec id: {}", dataFileInfo.getPath(), getOptimizeType(),
            dataFileInfo.specId);
        return null;
      }

      ContentFile<?> dataFile = ContentFileUtil.buildContentFile(dataFileInfo, partitionSpec);
      currentPartitions.add(partition);
      allPartitions.add(partition);
      if (isOptimized(dataFile, partition)) {
        return null;
      } else {
        if (!anyTaskRunning(partition)) {
          FileTree treeRoot =
              partitionFileTree.computeIfAbsent(partition, p -> FileTree.newTreeRoot());
          treeRoot.putNodeIfAbsent(DataTreeNode.of(dataFileInfo.getMask(), dataFileInfo.getIndex()))
              .addFile(dataFile, DataFileType.valueOf(dataFileInfo.getType()));

          // fill eq delete file map
          if (Objects.equals(dataFileInfo.getType(), DataFileType.EQ_DELETE_FILE.name())) {
            List<DataFile> files = partitionDeleteFiles.computeIfAbsent(partition, e -> new ArrayList<>());
            files.add((DataFile) dataFile);
            partitionDeleteFiles.put(partition, files);
          }

          addCnt.getAndIncrement();
        }
        return (DataFile) dataFile;
      }
    }).filter(Objects::nonNull).collect(Collectors.toList());

    LOG.debug("{} ==== {} add {} change files into tree, total files: {}." + " After added, partition cnt of tree: {}",
        tableId(), getOptimizeType(), addCnt, changeOptimizeFiles.size(), partitionFileTree.size());
  }

  private void addBaseFileIntoFileTree() {
    LOG.debug("{} start plan base files", tableId());
    KeyedTable keyedArcticTable = arcticTable.asKeyedTable();

    AtomicInteger addCnt = new AtomicInteger();
    List<DataFileInfo> baseFileList = new ArrayList<>();
    baseFileList.addAll(ImmutableList.copyOf(baseTableFileList));
    baseFileList.addAll(ImmutableList.copyOf(posDeleteFileList));
    List<ContentFile<?>> baseOptimizeFiles = baseFileList.stream().map(dataFileInfo -> {
      PartitionSpec partitionSpec = keyedArcticTable.baseTable().specs().get((int) dataFileInfo.getSpecId());
      String partition = dataFileInfo.getPartition() == null ? "" : dataFileInfo.getPartition();

      if (partitionSpec == null) {
        LOG.error("{} {} can not find partitionSpec id: {}", dataFileInfo.getPath(), getOptimizeType(),
            dataFileInfo.specId);
        return null;
      }

      ContentFile<?> contentFile = ContentFileUtil.buildContentFile(dataFileInfo, partitionSpec);
      currentPartitions.add(partition);
      allPartitions.add(partition);
      if (!anyTaskRunning(partition)) {
        FileTree treeRoot =
            partitionFileTree.computeIfAbsent(partition, p -> FileTree.newTreeRoot());
        treeRoot.putNodeIfAbsent(DataTreeNode.of(dataFileInfo.getMask(), dataFileInfo.getIndex()))
            .addFile(contentFile, DataFileType.valueOf(dataFileInfo.getType()).equals(DataFileType.POS_DELETE_FILE) ?
                DataFileType.POS_DELETE_FILE : DataFileType.BASE_FILE);

        // fill node position delete file map
        if (contentFile instanceof DeleteFile) {
          List<DeleteFile> files = partitionPosDeleteFiles.computeIfAbsent(partition, e -> new ArrayList<>());
          files.add((DeleteFile) contentFile);
          partitionPosDeleteFiles.put(partition, files);
        }

        addCnt.getAndIncrement();
      }
      return contentFile;
    }).filter(Objects::nonNull).collect(Collectors.toList());

    LOG.debug("{} ==== {} add {} base files into tree, total files: {}." + " After added, partition cnt of tree: {}",
        tableId(), getOptimizeType(), addCnt, baseOptimizeFiles.size(), partitionFileTree.size());
  }

  private List<BaseOptimizeTask> collectKeyedTableTasks(String partition, FileTree treeRoot) {
    List<BaseOptimizeTask> collector = new ArrayList<>();
    String group = UUID.randomUUID().toString();
    long createTime = System.currentTimeMillis();
    TaskConfig taskPartitionConfig = new TaskConfig(partition, changeTableMaxTransactionId.get(partition),
        group, historyId, OptimizeType.Minor, createTime);
    treeRoot.completeTree(false);
    List<FileTree> subTrees = new ArrayList<>();
    // split tasks
    treeRoot.splitSubTree(subTrees, new CanSplitFileTree());
    for (FileTree subTree : subTrees) {
      List<DataFile> insertFiles = new ArrayList<>();
      List<DataFile> deleteFiles = new ArrayList<>();
      List<DataFile> baseFiles = new ArrayList<>();
      subTree.collectInsertFiles(insertFiles);
      subTree.collectDeleteFiles(deleteFiles);
      subTree.collectBaseFiles(baseFiles);
      List<DataTreeNode> sourceNodes = new ArrayList<>();
      if (CollectionUtils.isNotEmpty(baseFiles)) {
        sourceNodes = Collections.singletonList(subTree.getNode());
      }
      Set<DataTreeNode> baseFileNodes = baseFiles.stream()
          .map(dataFile -> DefaultKeyedFile.parseMetaFromFileName(dataFile.path().toString()).node())
          .collect(Collectors.toSet());
      List<DeleteFile> posDeleteFiles = partitionPosDeleteFiles
          .computeIfAbsent(partition, e -> Collections.emptyList()).stream()
          .filter(deleteFile ->
              baseFileNodes.contains(DefaultKeyedFile.parseMetaFromFileName(deleteFile.path().toString()).node()))
          .collect(Collectors.toList());
      // if no insert files and no eq-delete file, skip
      if (CollectionUtils.isEmpty(insertFiles) && CollectionUtils.isEmpty(deleteFiles)) {
        continue;
      }
      collector.add(buildOptimizeTask(sourceNodes,
          insertFiles, deleteFiles, baseFiles, posDeleteFiles, taskPartitionConfig));
    }

    return collector;
  }

  private boolean isOptimized(ContentFile<?> dataFile, String partition) {
    // if Pos-Delete files, ignore
    if (dataFile instanceof DeleteFile) {
      return false;
    }

    Long currentValue = changeTableMaxTransactionId.get(partition);
    long transactionId = DefaultKeyedFile.parseMetaFromFileName(dataFile.path().toString()).transactionId();
    if (currentValue == null) {
      changeTableMaxTransactionId.put(partition, transactionId);
    } else {
      if (currentValue < transactionId) {
        changeTableMaxTransactionId.put(partition, transactionId);
      }
    }
    return transactionId <= getBaseMaxTransactionId(partition);
  }

  private long getBaseMaxTransactionId(String partition) {
    if (baseTableMaxTransactionId == null) {
      baseTableMaxTransactionId = new HashMap<>();
      baseTableMaxTransactionId.putAll(arcticTable.asKeyedTable().baseTable().maxTransactionId());
      LOG.debug("{} ==== get base table max transaction id: {}", tableId(), baseTableMaxTransactionId);
    }
    Long maxTransactionId = baseTableMaxTransactionId.get(partition);
    return maxTransactionId == null ? -1 : maxTransactionId;
  }

  static class CanSplitFileTree implements Predicate<FileTree> {

    /**
     * file tree can't split:
     * - root node is leaf node
     * - root node contains any base/insert/delete files
     * .
     *
     * @param fileTree - file tree to split
     * @return true if this fileTree need split
     */
    @Override
    public boolean test(FileTree fileTree) {
      if (fileTree.getLeft() == null && fileTree.getRight() == null) {
        return false;
      }

      return fileTree.getBaseFiles().isEmpty() &&
          fileTree.getInsertFiles().isEmpty() &&
          fileTree.getDeleteFiles().isEmpty() &&
          fileTree.findAnyBaseFilesInTree();
    }
  }
}
