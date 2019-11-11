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


import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * Reason:
 * Date: 2018/7/20
 * Company: www.dtstack.com
 * @author xuchao
 */

public class JoinScope {

    private List<ScopeChild> children = Lists.newArrayList();

    private Map<String, ScopeChild> aliasMap = Maps.newHashMap();

    public void addScope(ScopeChild scopeChild){
        children.add(scopeChild);
        aliasMap.put(scopeChild.getAlias(), scopeChild);
    }

    public ScopeChild getScope(String tableAlias){
        return aliasMap.get(tableAlias);
    }

    public List<ScopeChild> getChildren() {
        return children;
    }

    public TypeInformation getFieldType(String tableName, String fieldName){
        ScopeChild scopeChild = aliasMap.get(tableName);
        if(scopeChild == null){
            throw new RuntimeException("can't find ");
        }

        RowTypeInfo rowTypeInfo = scopeChild.getRowTypeInfo();
        int index = rowTypeInfo.getFieldIndex(fieldName);
        if(index == -1){
            throw new RuntimeException("can't find field: " + fieldName);
        }

        return rowTypeInfo.getTypeAt(index);
    }

    public static class ScopeChild{

        private String alias;

        private String tableName;

        private RowTypeInfo rowTypeInfo;

        public String getAlias() {
            return alias;
        }

        public void setAlias(String alias) {
            this.alias = alias;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public RowTypeInfo getRowTypeInfo() {
            return rowTypeInfo;
        }

        public void setRowTypeInfo(RowTypeInfo rowTypeInfo) {
            this.rowTypeInfo = rowTypeInfo;
        }
    }
}
