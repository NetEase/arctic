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

import com.netease.arctic.ams.server.utils.AmsUtils;

import java.sql.Timestamp;

public class PartitionBaseInfo {
  String partition;
  int fileCount;
  long fileSize;
  Timestamp lastCommitTime;

  // parameters needed for front-end only
  String size;

  public PartitionBaseInfo(String partition, int fileCount, long fileSize, Timestamp lastCommitTime) {
    this.partition = partition;
    this.fileCount = fileCount;
    this.fileSize = fileSize;
    this.lastCommitTime = lastCommitTime;
    this.size = AmsUtils.byteToXB(fileSize);
  }

  public String getPartition() {
    return partition;
  }

  public void setPartition(String partition) {
    this.partition = partition;
  }

  public int getFileCount() {
    return fileCount;
  }

  public void setFileCount(int fileCount) {
    this.fileCount = fileCount;
  }

  public long getFileSize() {
    return fileSize;
  }

  public void setFileSize(long fileSize) {
    this.fileSize = fileSize;
    this.size = AmsUtils.byteToXB(fileSize);

  }

  public long getLastCommitTime() {
    return lastCommitTime.getTime();
  }

  public void setLastCommitTime(Timestamp lastCommitTime) {
    this.lastCommitTime = lastCommitTime;
  }

  public String getSize() {
    return this.size;
  }
}
