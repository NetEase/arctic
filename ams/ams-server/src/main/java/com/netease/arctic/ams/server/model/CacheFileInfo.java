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

package com.netease.arctic.ams.server.model;

import com.netease.arctic.ams.api.DataFileInfo;
import com.netease.arctic.ams.api.TableIdentifier;

public class CacheFileInfo {

  private TableIdentifier tableIdentifier;
  private Long addSnapshotId;
  private Long parentSnapshotId;
  private Long deleteSnapshotId;
  private String innerTable;
  private String filePath;
  // primaryKeyMd5 = md5(TableMetadataUtil.getTableAllIdentifyName(tableIdentifier)+innerTable+filePath)
  private String primaryKeyMd5;
  private String fileType;
  private Long fileSize;
  private Long fileMask;
  private Long fileIndex;
  private Long specId;
  private String partitionName;
  private Long commitTime;
  private Long recordCount;
  private String action;
  private Long watermark;

  public CacheFileInfo() {

  }

  public CacheFileInfo(String primaryKeyMd5, TableIdentifier tableIdentifier, Long addSnapshotId,
      Long parentSnapshotId, Long deleteSnapshotId, String innerTable,
      String filePath, String fileType, Long fileSize, Long fileMask, Long fileIndex, Long specId,
      String partitionName, Long commitTime, Long recordCount, String action, Long watermark) {
    this.primaryKeyMd5 = primaryKeyMd5;
    this.tableIdentifier = tableIdentifier;
    this.addSnapshotId = addSnapshotId;
    this.parentSnapshotId = parentSnapshotId;
    this.deleteSnapshotId = deleteSnapshotId;
    this.innerTable = innerTable;
    this.filePath = filePath;
    this.fileType = fileType;
    this.fileSize = fileSize;
    this.fileMask = fileMask;
    this.fileIndex = fileIndex;
    this.specId = specId;
    this.partitionName = partitionName;
    this.commitTime = commitTime;
    this.recordCount = recordCount;
    this.action = action;
    this.watermark = watermark;
  }

  public DataFileInfo toDataFileInfo() {
    DataFileInfo dataFileInfo = new DataFileInfo();
    dataFileInfo.setCommitTime(commitTime);
    dataFileInfo.setPath(filePath);
    dataFileInfo.setPartition(partitionName);
    dataFileInfo.setSize(fileSize);
    dataFileInfo.setType(fileType);

    return dataFileInfo;
  }

  public String getPrimaryKeyMd5() {
    return primaryKeyMd5;
  }

  public void setPrimaryKeyMd5(String primaryKeyMd5) {
    this.primaryKeyMd5 = primaryKeyMd5;
  }

  public Long getCommitTime() {
    return commitTime;
  }

  public void setCommitTime(Long commitTime) {
    this.commitTime = commitTime;
  }

  public String getPartitionName() {
    return partitionName;
  }

  public void setPartitionName(String partitionName) {
    this.partitionName = partitionName;
  }

  public TableIdentifier getTableIdentifier() {
    return tableIdentifier;
  }

  public void setTableIdentifier(TableIdentifier tableIdentifier) {
    this.tableIdentifier = tableIdentifier;
  }

  public Long getAddSnapshotId() {
    return addSnapshotId;
  }

  public void setAddSnapshotId(Long addSnapshotId) {
    this.addSnapshotId = addSnapshotId;
  }

  public Long getParentSnapshotId() {
    return parentSnapshotId;
  }

  public void setParentSnapshotId(Long parentSnapshotId) {
    this.parentSnapshotId = parentSnapshotId;
  }

  public Long getDeleteSnapshotId() {
    return deleteSnapshotId;
  }

  public void setDeleteSnapshotId(Long deleteSnapshotId) {
    this.deleteSnapshotId = deleteSnapshotId;
  }

  public String getInnerTable() {
    return innerTable;
  }

  public void setInnerTable(String innerTable) {
    this.innerTable = innerTable;
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public String getFileType() {
    return fileType;
  }

  public void setFileType(String fileType) {
    this.fileType = fileType;
  }

  public Long getFileSize() {
    return fileSize;
  }

  public void setFileSize(Long fileSize) {
    this.fileSize = fileSize;
  }

  public Long getFileMask() {
    return fileMask;
  }

  public void setFileMask(Long fileMask) {
    this.fileMask = fileMask;
  }

  public Long getFileIndex() {
    return fileIndex;
  }

  public void setFileIndex(Long fileIndex) {
    this.fileIndex = fileIndex;
  }

  public Long getSpecId() {
    return specId;
  }

  public void setSpecId(Long specId) {
    this.specId = specId;
  }

  public Long getRecordCount() {
    return recordCount;
  }

  public void setRecordCount(Long recordCount) {
    this.recordCount = recordCount;
  }

  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = action;
  }

  public Long getWatermark() {
    return watermark;
  }

  public void setWatermark(Long watermark) {
    this.watermark = watermark;
  }

  @Override
  public String toString() {
    return "CacheFileInfo{" +
        "tableIdentifier=" + tableIdentifier +
        ", addSnapshotId=" + addSnapshotId +
        ", deleteSnapshotId=" + deleteSnapshotId +
        ", innerTable='" + innerTable + '\'' +
        ", filePath='" + filePath + '\'' +
        ", fileType='" + fileType + '\'' +
        ", fileSize=" + fileSize +
        ", fileMask=" + fileMask +
        ", fileIndex=" + fileIndex +
        ", specId=" + specId +
        ", partitionName='" + partitionName + '\'' +
        ", commitTime=" + commitTime +
        ", recordCount=" + recordCount +
        ", action='" + action + '\'' +
        ", watermark=" + watermark +
        '}';
  }
}
