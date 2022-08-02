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

import com.netease.arctic.ams.api.OptimizeStatus;
import com.netease.arctic.ams.api.TreeNode;
import com.netease.arctic.ams.server.model.BaseOptimizeTask;
import com.netease.arctic.ams.server.model.BaseOptimizeTaskRuntime;
import com.netease.arctic.ams.server.model.TableOptimizeRuntime;
import com.netease.arctic.data.DefaultKeyedFile;
import com.netease.arctic.table.TableProperties;
import com.netease.arctic.utils.SerializationUtil;
import org.apache.iceberg.DataFile;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TestMajorOptimizeCommit extends TestMajorOptimizePlan {

  @Test
  public void testMajorOptimizeCommit() throws Exception {
    insertBasePosDeleteFiles(2);

    Set<String> oldDataFilesPath = new HashSet<>();
    Set<String> oldDeleteFilesPath = new HashSet<>();
    testKeyedTable.baseTable().newScan().planFiles()
        .forEach(fileScanTask -> {
          oldDataFilesPath.add((String) fileScanTask.file().path());
          fileScanTask.deletes().forEach(deleteFile -> oldDeleteFilesPath.add((String) deleteFile.path()));
        });

    //testKeyedTable.properties().put(TableProperties.MAJOR_OPTIMIZE_TRIGGER_DELETE_FILE_SIZE_BYTES, "0");
    testKeyedTable.updateProperties().
        set(TableProperties.MAJOR_OPTIMIZE_TRIGGER_DELETE_FILE_SIZE_BYTES, "0").commit();
    TableOptimizeRuntime tableOptimizeRuntime = new TableOptimizeRuntime(testKeyedTable.id());
    MajorOptimizePlan majorOptimizePlan = new MajorOptimizePlan(testKeyedTable,
        tableOptimizeRuntime, baseDataFilesInfo, posDeleteFilesInfo,
        new HashMap<>(), 1, System.currentTimeMillis(), snapshotId -> true);
    List<BaseOptimizeTask> tasks = majorOptimizePlan.plan();

    Map<TreeNode, List<DataFile>> resultFiles = generateTargetFiles();
    List<OptimizeTaskItem> taskItems = tasks.stream().map(task -> {
      BaseOptimizeTaskRuntime optimizeRuntime = new BaseOptimizeTaskRuntime(task.getTaskId());
      List<DataFile> targetFiles = resultFiles.get(task.getSourceNodes().get(0));
      optimizeRuntime.setPreparedTime(System.currentTimeMillis());
      optimizeRuntime.setStatus(OptimizeStatus.Prepared);
      optimizeRuntime.setReportTime(System.currentTimeMillis());
      optimizeRuntime.setNewFileCnt(targetFiles == null ? 0 : targetFiles.size());
      if (targetFiles != null) {
        optimizeRuntime.setNewFileSize(targetFiles.get(0).fileSizeInBytes());
        optimizeRuntime.setTargetFiles(targetFiles.stream().map(SerializationUtil::toByteBuffer).collect(Collectors.toList()));
      }
      // 1min
      optimizeRuntime.setCostTime(60 * 1000);
      return new OptimizeTaskItem(task, optimizeRuntime);
    }).collect(Collectors.toList());
    Map<String, List<OptimizeTaskItem>> partitionTasks = taskItems.stream()
        .collect(Collectors.groupingBy(taskItem -> taskItem.getOptimizeTask().getPartition()));

    BaseOptimizeCommit optimizeCommit = new BaseOptimizeCommit(testKeyedTable, partitionTasks);
    optimizeCommit.commit(tableOptimizeRuntime);

    Set<String> newDataFilesPath = new HashSet<>();
    Set<String> newDeleteFilesPath = new HashSet<>();
    testKeyedTable.baseTable().newScan().planFiles()
        .forEach(fileScanTask -> {
          newDataFilesPath.add((String) fileScanTask.file().path());
          fileScanTask.deletes().forEach(deleteFile -> newDeleteFilesPath.add((String) deleteFile.path()));
        });
    Assert.assertNotEquals(oldDataFilesPath, newDataFilesPath);
    Assert.assertNotEquals(oldDeleteFilesPath, newDeleteFilesPath);
  }

  private Map<TreeNode, List<DataFile>> generateTargetFiles() throws Exception {
    List<DataFile> dataFiles = insertKeyedTableBaseDataFiles(3);
    return dataFiles.stream().collect(Collectors.groupingBy(dataFile ->  {
      DefaultKeyedFile keyedFile = new DefaultKeyedFile(dataFile);
      return keyedFile.node().toAmsTreeNode();
    }));
  }
}
