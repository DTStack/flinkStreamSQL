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

 

package com.dtstack.flink.sql.side;

import com.google.common.collect.HashBasedTable;
import org.apache.commons.lang3.StringUtils;

/**
 * 用于记录转换之后的表和原来表直接字段的关联关系
 * Date: 2018/8/30
 * Company: www.dtstack.com
 * @author xuchao
 */

public class FieldReplaceInfo {

    private HashBasedTable<String, String, String> mappingTable;

    private String targetTableName = null;

    private String targetTableAlias = null;

    private FieldReplaceInfo preNode = null;

    public void setMappingTable(HashBasedTable<String, String, String> mappingTable) {
        this.mappingTable = mappingTable;
    }

    public HashBasedTable<String, String, String> getMappingTable() {
        return mappingTable;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    public String getTargetTableAlias() {
        return targetTableAlias;
    }

    public FieldReplaceInfo getPreNode() {
        return preNode;
    }

    public void setPreNode(FieldReplaceInfo preNode) {
        this.preNode = preNode;
    }

    public void setTargetTableAlias(String targetTableAlias) {
        this.targetTableAlias = targetTableAlias;
    }

    /**
     * 根据原始的tableName + fieldName 获取转换之后的fieldName
     * @param tableName
     * @param fieldName
     * @return
     */
    public String getTargetFieldName(String tableName, String fieldName){
        String targetFieldName = mappingTable.get(tableName, fieldName);
        if(StringUtils.isNotBlank(targetFieldName)){
            return targetFieldName;
        }

        if(preNode == null){
            return null;
        }

        String preNodeTargetFieldName = preNode.getTargetFieldName(tableName, fieldName);
        if(StringUtils.isBlank(preNodeTargetFieldName)){
            return null;
        }

        return mappingTable.get(preNode.getTargetTableName(), preNodeTargetFieldName);
    }
}
