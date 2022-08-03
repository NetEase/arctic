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

package com.netease.arctic.ams.server.mapper;

import com.netease.arctic.ams.api.TableIdentifier;
import com.netease.arctic.ams.server.model.CacheSnapshotInfo;
import com.netease.arctic.ams.server.model.SnapshotStatistics;
import com.netease.arctic.ams.server.mybatis.Long2TsConvertor;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface SnapInfoCacheMapper {
  String TABLE_NAME = "snapshot_info_cache";

  @Insert("insert into " + TABLE_NAME + " (table_identifier, snapshot_id, parent_snapshot_id, action," +
      " inner_table, commit_time)" +
      " values(#{cacheFileInfo.tableIdentifier," +
      "typeHandler=com.netease.arctic.ams.server.mybatis.TableIdentifier2StringConverter}," +
      " #{cacheFileInfo.snapshotId}, #{cacheFileInfo.parentSnapshotId}, #{cacheFileInfo.action}," +
      " #{cacheFileInfo.innerTable}, #{cacheFileInfo.commitTime, typeHandler=com.netease.arctic.ams.server" +
      ".mybatis.Long2TsConvertor})")
  void insertCache(@Param("cacheFileInfo") CacheSnapshotInfo info);

  @Select("select snapshot_id,parent_snapshot_id, commit_time from " + TABLE_NAME + " where table_identifier = " +
      "#{tableIdentifier," +
      " typeHandler=com.netease.arctic.ams.server.mybatis.TableIdentifier2StringConverter} and inner_table = " +
      "#{type} and commit_time = (select max(commit_time) from snapshot_info_cache where table_identifier = " +
      "#{tableIdentifier, typeHandler=com.netease.arctic.ams.server.mybatis.TableIdentifier2StringConverter} and " +
      "inner_table = #{type})")
  @Results({
      @Result(column = "snapshot_id", property = "id"),
      @Result(column = "parent_snapshot_id", property = "parentId"),
      @Result(column = "commit_time", property = "commitTime",
          typeHandler = Long2TsConvertor.class)
  })
  List<SnapshotStatistics> getCurrentSnapInfo(@Param("tableIdentifier") TableIdentifier tableIdentifier,
                                              @Param("type") String tableType);

  @Delete("delete from " + TABLE_NAME + " where commit_time < #{expiredTime, typeHandler=com.netease.arctic.ams" +
      ".server.mybatis.Long2TsConvertor}")
  void expireCache(@Param("expiredTime") long expiredTime);

  @Delete("delete from " + TABLE_NAME + " where table_identifier = #{tableIdentifier, typeHandler=com.netease.arctic" +
      ".ams.server.mybatis.TableIdentifier2StringConverter}")
  void deleteTableCache(@Param("tableIdentifier") TableIdentifier tableIdentifier);
}