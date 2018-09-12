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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

/**
 *FIXME 需要考虑是直接返回所有的字段然后在外层再包裹原先的查询？
 * Date: 2018/7/20
 * Company: www.dtstack.com
 * @author xuchao
 */

public class ParserJoinField {

    /**
     * 需要解析出selectlist和where中的字段信息
     * @return
     */
    public static List<FieldInfo> getRowTypeInfo(SqlNode sqlNode, JoinScope scope, boolean getAll){

        if(sqlNode.getKind() != SqlKind.SELECT){
            throw new RuntimeException("------not select node--------\n" + sqlNode.toString());
        }

        List<FieldInfo> fieldInfoList = Lists.newArrayList();
        if(getAll){
            return getAllField(scope);
        }

        SqlSelect sqlSelect = (SqlSelect)sqlNode;
        SqlNodeList sqlNodeList = sqlSelect.getSelectList();
        for(SqlNode fieldNode : sqlNodeList.getList()){
            SqlIdentifier identifier = (SqlIdentifier)fieldNode;
            if(!identifier.isStar()) {
                System.out.println(identifier);
                String tableName = identifier.getComponent(0).getSimple();
                String fieldName = identifier.getComponent(1).getSimple();
                TypeInformation<?> type = scope.getFieldType(tableName, fieldName);
                FieldInfo fieldInfo = new FieldInfo();
                fieldInfo.setTable(tableName);
                fieldInfo.setFieldName(fieldName);
                fieldInfo.setTypeInformation(type);
                fieldInfoList.add(fieldInfo);
            } else {
                //处理
                System.out.println("----------");
                int identifierSize = identifier.names.size();

                switch(identifierSize) {
                    case 1:
                        fieldInfoList.addAll(getAllField(scope));
                    default:
                        SqlIdentifier tableIdentify = identifier.skipLast(1);
                        JoinScope.ScopeChild scopeChild = scope.getScope(tableIdentify.getSimple());
                        if(scopeChild == null){
                            throw new RuntimeException("can't find table alias " + tableIdentify.getSimple());
                        }

                        RowTypeInfo field = scopeChild.getRowTypeInfo();
                        String[] fieldNames = field.getFieldNames();
                        TypeInformation<?>[] types = field.getFieldTypes();
                        for(int i=0; i< field.getTotalFields(); i++){
                            String fieldName = fieldNames[i];
                            TypeInformation<?> type = types[i];
                            FieldInfo fieldInfo = new FieldInfo();
                            fieldInfo.setTable(tableIdentify.getSimple());
                            fieldInfo.setFieldName(fieldName);
                            fieldInfo.setTypeInformation(type);
                            fieldInfoList.add(fieldInfo);
                        }
                }
            }
        }

        return fieldInfoList;
    }

    private static List<FieldInfo> getAllField(JoinScope scope){
        Iterator prefixId = scope.getChildren().iterator();
        List<FieldInfo> fieldInfoList = Lists.newArrayList();
        while(true) {
            JoinScope.ScopeChild resolved;
            RowTypeInfo field;
            if(!prefixId.hasNext()) {
                return fieldInfoList;
            }

            resolved = (JoinScope.ScopeChild)prefixId.next();
            field = resolved.getRowTypeInfo();
            String[] fieldNames = field.getFieldNames();
            TypeInformation<?>[] types = field.getFieldTypes();
            for(int i=0; i< field.getTotalFields(); i++){
                String fieldName = fieldNames[i];
                TypeInformation<?> type = types[i];
                FieldInfo fieldInfo = new FieldInfo();
                fieldInfo.setTable(resolved.getAlias());
                fieldInfo.setFieldName(fieldName);
                fieldInfo.setTypeInformation(type);
                fieldInfoList.add(fieldInfo);
            }
        }
    }

}
