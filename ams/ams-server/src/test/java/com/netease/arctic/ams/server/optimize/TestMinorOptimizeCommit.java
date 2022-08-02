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

import com.netease.arctic.ams.api.DataFileInfo;
import com.netease.arctic.ams.api.OptimizeStatus;
import com.netease.arctic.ams.api.TreeNode;
import com.netease.arctic.ams.server.model.BaseOptimizeTask;
import com.netease.arctic.ams.server.model.BaseOptimizeTaskRuntime;
import com.netease.arctic.ams.server.model.TableOptimizeRuntime;
import com.netease.arctic.ams.server.utils.JDBCSqlSessionFactoryProvider;
import com.netease.arctic.data.DataTreeNode;
import com.netease.arctic.data.DefaultKeyedFile;
import com.netease.arctic.utils.SerializationUtil;
import org.apache.iceberg.DeleteFile;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
    JDBCSqlSessionFactoryProvider.class
})
@PowerMockIgnore({"org.apache.logging.log4j.*", "javax.management.*", "org.apache.http.conn.ssl.*",
    "com.amazonaws.http.conn.ssl.*",
    "javax.net.ssl.*", "org.apache.hadoop.*", "javax.*", "com.sun.org.apache.*", "org.apache.xerces.*"})
public class TestMinorOptimizeCommit extends TestMinorOptimizePlan {

  @Before
  public void mock() {
    mockStatic(JDBCSqlSessionFactoryProvider.class);
    when(JDBCSqlSessionFactoryProvider.get()).thenReturn(null);
  }

  @Test
  public void testMinorOptimizeCommit() throws Exception {
    insertBasePosDeleteFiles(2);
    insertChangeDeleteFiles(3);
    insertChangeDataFiles(4);

    Set<String> oldDataFilesPath = new HashSet<>();
    Set<String> oldDeleteFilesPath = new HashSet<>();
    testKeyedTable.baseTable().newScan().planFiles()
        .forEach(fileScanTask -> {
          oldDataFilesPath.add((String) fileScanTask.file().path());
          fileScanTask.deletes().forEach(deleteFile -> oldDeleteFilesPath.add((String) deleteFile.path()));
        });

    List<DataFileInfo> changeTableFilesInfo = new ArrayList<>(changeInsertFilesInfo);
    changeTableFilesInfo.addAll(changeDeleteFilesInfo);
    TableOptimizeRuntime tableOptimizeRuntime =  new TableOptimizeRuntime(testKeyedTable.id());
    MinorOptimizePlan minorOptimizePlan = new MinorOptimizePlan(testKeyedTable,
       tableOptimizeRuntime, baseDataFilesInfo, changeTableFilesInfo, posDeleteFilesInfo,
        new HashMap<>(), 1, System.currentTimeMillis(), snapshotId -> true);
    List<BaseOptimizeTask> tasks = minorOptimizePlan.plan();

    Map<TreeNode, List<DeleteFile>> resultFiles = generateTargetFiles();
    List<OptimizeTaskItem> taskItems = tasks.stream().map(task -> {
      BaseOptimizeTaskRuntime optimizeRuntime = new BaseOptimizeTaskRuntime(task.getTaskId());
      List<DeleteFile> targetFiles = resultFiles.get(task.getSourceNodes().get(0));
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

  private Map<TreeNode, List<DeleteFile>> generateTargetFiles() throws Exception {
    List<DeleteFile> deleteFiles = insertBasePosDeleteFiles(6);
    return deleteFiles.stream().collect(Collectors.groupingBy(deleteFile ->  {
      DataTreeNode dataTreeNode = DefaultKeyedFile.parseMetaFromFileName(deleteFile.path().toString()).node();
      return new TreeNode(dataTreeNode.mask(), dataTreeNode.index());
    }));
  }
}
