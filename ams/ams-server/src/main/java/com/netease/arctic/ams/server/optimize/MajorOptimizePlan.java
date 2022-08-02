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
import com.netease.arctic.table.TableProperties;
import com.netease.arctic.table.UnkeyedTable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.util.BinPacking;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class MajorOptimizePlan extends BaseOptimizePlan {
  private static final Logger LOG = LoggerFactory.getLogger(MajorOptimizePlan.class);

  // partition -> small file
  protected final Map<String, List<DataFile>> partitionSmallFiles = new LinkedHashMap<>();

  // partition -> current optimize is merge pos-delete or only merge small files
  protected final Map<String, Boolean> isMergePosDelete = new LinkedHashMap<>();

  public MajorOptimizePlan(ArcticTable arcticTable, TableOptimizeRuntime tableOptimizeRuntime,
                           List<DataFileInfo> baseTableFileList, List<DataFileInfo> posDeleteFileList,
                           Map<String, Boolean> partitionTaskRunning, int queueId, long currentTime,
                           Predicate<Long> snapshotIsCached) {
    super(arcticTable, tableOptimizeRuntime, baseTableFileList, Collections.emptyList(), posDeleteFileList,
        partitionTaskRunning, queueId, currentTime, snapshotIsCached);
  }

  @Override
  public void addOptimizeFilesTree() {
    addBaseFileIntoFileTree();
  }

  @Override
  protected OptimizeType getOptimizeType() {
    return OptimizeType.Major;
  }

  @Override
  public boolean partitionNeedPlan(String partitionToPath) {
    long current = System.currentTimeMillis();

    // check position delete file total size
    long posDeleteSize = posDeleteFileList.stream().mapToLong(DataFileInfo::getSize).sum();
    if (posDeleteSize >= PropertyUtil.propertyAsLong(arcticTable.properties(),
        TableProperties.MAJOR_OPTIMIZE_TRIGGER_DELETE_FILE_SIZE_BYTES,
        TableProperties.MAJOR_OPTIMIZE_TRIGGER_DELETE_FILE_SIZE_BYTES_DEFAULT)) {
      isMergePosDelete.put(partitionToPath, true);
      return true;
    }

    // check small data file count
    List<DataFile> dataFileList = partitionSmallFiles.get(partitionToPath);
    if (CollectionUtils.isNotEmpty(dataFileList)) {
      // file count
      if (dataFileList.size() >= PropertyUtil.propertyAsInt(arcticTable.properties(),
          TableProperties.MAJOR_OPTIMIZE_TRIGGER_SMALL_FILE_COUNT,
          TableProperties.MAJOR_OPTIMIZE_TRIGGER_SMALL_FILE_COUNT_DEFAULT)) {
        return true;
      }

      // optimize interval
      if (current - tableOptimizeRuntime.getLatestMajorOptimizeTime(partitionToPath) >=
          PropertyUtil.propertyAsLong(arcticTable.properties(), TableProperties.MAJOR_OPTIMIZE_TRIGGER_MAX_INTERVAL,
              TableProperties.MAJOR_OPTIMIZE_TRIGGER_MAX_INTERVAL_DEFAULT) && dataFileList.size() >= 2) {
        return true;
      }
    }

    LOG.debug("{} ==== don't need {} optimize plan, skip partition {}", tableId(), getOptimizeType(), partitionToPath);
    return false;
  }

  protected List<BaseOptimizeTask> collectTask(String partition) {
    List<BaseOptimizeTask> result;
    if (arcticTable.isUnkeyedTable()) {
      List<DataFile> fileList = partitionSmallFiles.computeIfAbsent(partition, e -> new ArrayList<>());
      result = collectUnKeyedTableTasks(partition, fileList);
      // init files
      partitionSmallFiles.put(partition, Collections.emptyList());
      partitionFileTree.get(partition).initFiles();
    } else {
      FileTree treeRoot = partitionFileTree.get(partition);
      result = collectKeyedTableTasks(partition, treeRoot);
      // init files
      partitionSmallFiles.put(partition, Collections.emptyList());
      partitionPosDeleteFiles.put(partition, Collections.emptyList());
      partitionFileTree.get(partition).initFiles();
    }

    return result;
  }

  public boolean isDeletePosDelete(String partition) {
    return isMergePosDelete.containsKey(partition);
  }

  private void addBaseFileIntoFileTree() {
    LOG.debug("{} start {} plan base files", tableId(), getOptimizeType());
    UnkeyedTable baseTable;
    if (arcticTable.isKeyedTable()) {
      baseTable = arcticTable.asKeyedTable().baseTable();
    } else {
      baseTable = arcticTable.asUnkeyedTable();
    }

    AtomicInteger addCnt = new AtomicInteger();
    List<DataFileInfo> baseFileList = new ArrayList<>();
    baseFileList.addAll(ImmutableList.copyOf(baseTableFileList));
    baseFileList.addAll(ImmutableList.copyOf(posDeleteFileList));
    List<ContentFile<?>> baseOptimizeFiles = baseFileList.stream().map(dataFileInfo -> {
      PartitionSpec partitionSpec = baseTable.specs().get((int) dataFileInfo.getSpecId());
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
            .addFile(contentFile,
                DataFileType.valueOf(dataFileInfo.getType()).equals(DataFileType.POS_DELETE_FILE) ?
                    DataFileType.POS_DELETE_FILE : DataFileType.BASE_FILE);

        // fill node position delete file map
        if (contentFile instanceof DeleteFile) {
          List<DeleteFile> files = partitionPosDeleteFiles.computeIfAbsent(partition, e -> new ArrayList<>());
          files.add((DeleteFile) contentFile);
          partitionPosDeleteFiles.put(partition, files);
        }

        // fill partition small file map
        if (contentFile instanceof DataFile &&
            dataFileInfo.getSize() < PropertyUtil.propertyAsLong(arcticTable.properties(),
            TableProperties.OPTIMIZE_SMALL_FILE_SIZE_BYTES_THRESHOLD,
                TableProperties.OPTIMIZE_SMALL_FILE_SIZE_BYTES_THRESHOLD_DEFAULT)) {
          List<DataFile> files = partitionSmallFiles.computeIfAbsent(partition, e -> new ArrayList<>());
          files.add((DataFile) contentFile);
          partitionSmallFiles.put(partition, files);
        }

        addCnt.getAndIncrement();
      }
      return contentFile;
    }).filter(Objects::nonNull).collect(Collectors.toList());

    LOG.debug("{} ==== {} add {} base files into tree, total files: {}." + " After added, partition cnt of tree: {}",
        tableId(), getOptimizeType(), addCnt, baseOptimizeFiles.size(), partitionFileTree.size());
  }

  private List<BaseOptimizeTask> collectUnKeyedTableTasks(String partition, List<DataFile> fileList) {
    List<BaseOptimizeTask> collector = new ArrayList<>();
    String group = UUID.randomUUID().toString();
    long createTime = System.currentTimeMillis();
    TaskConfig taskPartitionConfig = new TaskConfig(partition, null, group, historyId,
        OptimizeType.Major, createTime);

    long taskSize =
        PropertyUtil.propertyAsLong(arcticTable.properties(), TableProperties.MAJOR_OPTIMIZE_MAX_TASK_FILE_SIZE,
            TableProperties.MAJOR_OPTIMIZE_MAX_TASK_FILE_SIZE_DEFAULT);
    Long sum = fileList.stream().map(DataFile::fileSizeInBytes).reduce(0L, Long::sum);
    int taskCnt = (int) (sum / taskSize) + 1;
    List<List<DataFile>> packed = new BinPacking.ListPacker<DataFile>(taskSize, taskCnt, true)
        .pack(fileList, DataFile::fileSizeInBytes);
    for (List<DataFile> files : packed) {
      if (CollectionUtils.isNotEmpty(files)) {
        collector.add(buildOptimizeTask(null,
            Collections.emptyList(), Collections.emptyList(), files, Collections.emptyList(), taskPartitionConfig));
      }
    }
    return collector;
  }

  private List<BaseOptimizeTask> collectKeyedTableTasks(String partition, FileTree treeRoot) {
    List<BaseOptimizeTask> collector = new ArrayList<>();
    String group = UUID.randomUUID().toString();
    long createTime = System.currentTimeMillis();
    TaskConfig taskPartitionConfig = new TaskConfig(partition, null, group, historyId,
        OptimizeType.Major, createTime);
    treeRoot.completeTree(false);
    List<FileTree> subTrees = new ArrayList<>();
    // split tasks
    boolean isFilterSmallFiles = !isMergePosDelete.containsKey(partition);
    long smallFileSize = PropertyUtil.propertyAsLong(arcticTable.properties(),
        TableProperties.OPTIMIZE_SMALL_FILE_SIZE_BYTES_THRESHOLD,
        TableProperties.OPTIMIZE_SMALL_FILE_SIZE_BYTES_THRESHOLD_DEFAULT);
    treeRoot.splitSubTree(subTrees, new CanSplitFileTree(isFilterSmallFiles, smallFileSize));
    for (FileTree subTree : subTrees) {
      List<DataFile> baseFiles = new ArrayList<>();
      subTree.collectBaseFiles(baseFiles, isFilterSmallFiles, smallFileSize);
      if (!baseFiles.isEmpty()) {
        List<DataTreeNode> sourceNodes = Collections.singletonList(subTree.getNode());
        Set<DataTreeNode> baseFileNodes = baseFiles.stream()
            .map(dataFile -> DefaultKeyedFile.parseMetaFromFileName(dataFile.path().toString()).node())
            .collect(Collectors.toSet());
        List<DeleteFile> posDeleteFiles = partitionPosDeleteFiles
            .computeIfAbsent(partition, e -> Collections.emptyList()).stream()
            .filter(deleteFile ->
                baseFileNodes.contains(DefaultKeyedFile.parseMetaFromFileName(deleteFile.path().toString()).node()))
            .collect(Collectors.toList());
        // if only one base file and no position delete file, skip
        if (CollectionUtils.isEmpty(posDeleteFiles) && baseFiles.size() <= 1) {
          continue;
        }
        collector.add(buildOptimizeTask(sourceNodes,
            Collections.emptyList(), Collections.emptyList(), baseFiles, posDeleteFiles, taskPartitionConfig));
      }
    }

    return collector;
  }

  static class CanSplitFileTree implements Predicate<FileTree> {

    private final boolean isFilterSmallFiles;
    private final long smallFileSize;

    public CanSplitFileTree(boolean isFilterSmallFiles, long smallFileSize) {
      this.isFilterSmallFiles = isFilterSmallFiles;
      this.smallFileSize = smallFileSize;
    }

    /**
     * file tree can't split:
     * - root node is leaf node
     * - root node contains any base files
     *
     * @param fileTree - file tree to split
     * @return true if this fileTree need split
     */
    @Override
    public boolean test(FileTree fileTree) {
      if (fileTree.getLeft() == null && fileTree.getRight() == null) {
        return false;
      }
      List<DataFile> baseFiles = fileTree.getBaseFiles();
      if (isFilterSmallFiles) {
        baseFiles = baseFiles.stream()
            .filter(dataFile -> dataFile.fileSizeInBytes() < smallFileSize).collect(Collectors.toList());
      }
      return baseFiles.isEmpty();
    }
  }
}
