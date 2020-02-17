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


package com.dtstack.flink.sql.util;

import com.dtstack.flink.sql.side.FieldInfo;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.flink.table.api.Table;

import java.util.List;
import java.util.Map;

/**
 *
 * Date: 2020/2/17
 * Company: www.dtstack.com
 * @author xuchao
 */

public class TableUtils {

    /**
     * 获取select 的字段
     * @param sqlSelect
     */
    public static List<FieldInfo> parserSelectField(SqlSelect sqlSelect, Map<String, Table> localTableCache){
        SqlNodeList sqlNodeList = sqlSelect.getSelectList();
        List<FieldInfo> fieldInfoList = Lists.newArrayList();

        for(SqlNode fieldNode : sqlNodeList.getList()){
            SqlIdentifier identifier = (SqlIdentifier)fieldNode;
            if(!identifier.isStar()) {
                System.out.println(identifier);
                String tableName = identifier.getComponent(0).getSimple();
                String fieldName = identifier.getComponent(1).getSimple();
                FieldInfo fieldInfo = new FieldInfo();
                fieldInfo.setTable(tableName);
                fieldInfo.setFieldName(fieldName);
                fieldInfoList.add(fieldInfo);
            } else {
                //处理
                int identifierSize = identifier.names.size();

                switch(identifierSize) {
                    case 1:
                        throw new RuntimeException("not support to parse * without scope of table");
                    default:
                        SqlIdentifier tableIdentify = identifier.skipLast(1);
                        Table registerTable = localTableCache.get(tableIdentify.getSimple());
                        if(registerTable == null){
                            throw new RuntimeException("can't find table alias " + tableIdentify.getSimple());
                        }

                        String[] fieldNames = registerTable.getSchema().getFieldNames();
                        for(String fieldName : fieldNames){
                            FieldInfo fieldInfo = new FieldInfo();
                            fieldInfo.setTable(tableIdentify.getSimple());
                            fieldInfo.setFieldName(fieldName);
                            fieldInfoList.add(fieldInfo);
                        }
                }
            }
        }

        return fieldInfoList;
    }

}
