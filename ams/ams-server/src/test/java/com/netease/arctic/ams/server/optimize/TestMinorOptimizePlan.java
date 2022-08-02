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
import com.netease.arctic.ams.server.model.BaseOptimizeTask;
import com.netease.arctic.ams.server.model.TableOptimizeRuntime;
import com.netease.arctic.ams.server.util.DataFileInfoUtils;
import com.netease.arctic.data.ChangeAction;
import com.netease.arctic.io.writer.GenericChangeTaskWriter;
import com.netease.arctic.io.writer.GenericTaskWriters;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.WriteResult;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class TestMinorOptimizePlan extends TestBaseOptimizePlan {
  protected List<DataFileInfo> changeInsertFilesInfo = new ArrayList<>();
  protected List<DataFileInfo> changeDeleteFilesInfo = new ArrayList<>();

  @Test
  public void testMinorOptimize() throws IOException {
    insertBasePosDeleteFiles(2);
    insertChangeDeleteFiles(3);
    insertChangeDataFiles(4);

    List<DataFileInfo> changeTableFilesInfo = new ArrayList<>(changeInsertFilesInfo);
    changeTableFilesInfo.addAll(changeDeleteFilesInfo);
    MinorOptimizePlan minorOptimizePlan = new MinorOptimizePlan(testKeyedTable,
        new TableOptimizeRuntime(testKeyedTable.id()), baseDataFilesInfo, changeTableFilesInfo, posDeleteFilesInfo,
        new HashMap<>(), 1, System.currentTimeMillis(), snapshotId -> true);
    List<BaseOptimizeTask> tasks = minorOptimizePlan.plan();
    Assert.assertEquals(4, tasks.size());
    Assert.assertEquals(10, tasks.get(0).getBaseFiles().size());
    Assert.assertEquals(1, tasks.get(0).getPosDeleteFiles().size());
    Assert.assertEquals(10, tasks.get(0).getInsertFileCnt());
    Assert.assertEquals(10, tasks.get(0).getDeleteFileCnt());
    Assert.assertEquals(0, tasks.get(0).getIsDeletePosDelete());
  }

  protected void insertChangeDeleteFiles(long transactionId) throws IOException {
    GenericChangeTaskWriter writer = GenericTaskWriters.builderFor(testKeyedTable)
        .withChangeAction(ChangeAction.DELETE)
        .withTransactionId(transactionId).buildChangeWriter();

    List<DataFile> changeDeleteFiles = new ArrayList<>();
    // delete 1000 records in 2 partitions(2022-1-1\2022-1-2)
    int length = 100;
    for (int i = 1; i < length * 10; i = i + length) {
      for (Record record : baseRecords(i, length)) {
        writer.write(record);
      }
      WriteResult result = writer.complete();
      changeDeleteFiles.addAll(Arrays.asList(result.dataFiles()));
    }
    AppendFiles baseAppend = testKeyedTable.changeTable().newAppend();
    changeDeleteFiles.forEach(baseAppend::appendFile);
    baseAppend.commit();
    long commitTime = System.currentTimeMillis();

    changeDeleteFilesInfo = changeDeleteFiles.stream()
        .map(deleteFile -> DataFileInfoUtils.convertToDatafileInfo(deleteFile, commitTime, testKeyedTable))
        .collect(Collectors.toList());
  }

  protected void insertChangeDataFiles(long transactionId) throws IOException {
    GenericChangeTaskWriter writer = GenericTaskWriters.builderFor(testKeyedTable)
        .withChangeAction(ChangeAction.INSERT)
        .withTransactionId(transactionId).buildChangeWriter();

    List<DataFile> changeInsertFiles = new ArrayList<>();
    // write 1000 records to 2 partitions(2022-1-1\2022-1-2)
    int length = 100;
    for (int i = 1; i < length * 10; i = i + length) {
      for (Record record : baseRecords(i, length)) {
        writer.write(record);
      }
      WriteResult result = writer.complete();
      changeInsertFiles.addAll(Arrays.asList(result.dataFiles()));
    }
    AppendFiles baseAppend = testKeyedTable.changeTable().newAppend();
    changeInsertFiles.forEach(baseAppend::appendFile);
    baseAppend.commit();
    long commitTime = System.currentTimeMillis();

    changeInsertFilesInfo = changeInsertFiles.stream()
        .map(dataFile -> DataFileInfoUtils.convertToDatafileInfo(dataFile, commitTime, testKeyedTable))
        .collect(Collectors.toList());
  }
}
