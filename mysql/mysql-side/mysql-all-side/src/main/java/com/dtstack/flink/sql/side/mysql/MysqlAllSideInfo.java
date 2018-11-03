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

package com.dtstack.flink.sql.side.mysql;

import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.SideInfo;
import com.dtstack.flink.sql.side.SideTableInfo;
import com.dtstack.flink.sql.side.mysql.table.MysqlSideTableInfo;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;

import java.util.List;

/**
 * Reason:
 * Date: 2018/9/19
 * Company: www.dtstack.com
 * @author xuchao
 */

public class MysqlAllSideInfo extends SideInfo{

    private static final long serialVersionUID = -5858335638589472158L;

    public MysqlAllSideInfo(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    @Override
    public void buildEqualInfo(JoinInfo joinInfo, SideTableInfo sideTableInfo) {
        MysqlSideTableInfo mysqlSideTableInfo = (MysqlSideTableInfo) sideTableInfo;

        sqlCondition = "select ${selectField} from ${tableName} ";
        sqlCondition = sqlCondition.replace("${tableName}", mysqlSideTableInfo.getTableName()).replace("${selectField}", sideSelectFields);
        System.out.println("---------side_exe_sql-----\n" + sqlCondition);
    }

    @Override
    public void parseSelectFields(JoinInfo joinInfo){
        String sideTableName = joinInfo.getSideTableName();
        String nonSideTableName = joinInfo.getNonSideTable();
        List<String> fields = Lists.newArrayList();

        int sideIndex = 0;
        for( int i=0; i<outFieldInfoList.size(); i++){
            FieldInfo fieldInfo = outFieldInfoList.get(i);
            if(fieldInfo.getTable().equalsIgnoreCase(sideTableName)){
                fields.add(fieldInfo.getFieldName());
                sideFieldIndex.put(i, sideIndex);
                sideFieldNameIndex.put(i, fieldInfo.getFieldName());
                sideIndex++;
            }else if(fieldInfo.getTable().equalsIgnoreCase(nonSideTableName)){
                int nonSideIndex = rowTypeInfo.getFieldIndex(fieldInfo.getFieldName());
                inFieldIndex.put(i, nonSideIndex);
            }else{
                throw new RuntimeException("unknown table " + fieldInfo.getTable());
            }
        }

        if(fields.size() == 0){
            throw new RuntimeException("select non field from table " +  sideTableName);
        }

        //add join on condition field to select fields
        SqlNode conditionNode = joinInfo.getCondition();

        List<SqlNode> sqlNodeList = Lists.newArrayList();
        if(conditionNode.getKind() == SqlKind.AND){
            sqlNodeList.addAll(Lists.newArrayList(((SqlBasicCall)conditionNode).getOperands()));
        }else{
            sqlNodeList.add(conditionNode);
        }

        for(SqlNode sqlNode : sqlNodeList){
            dealOneEqualCon(sqlNode, sideTableName);
        }

        if(CollectionUtils.isEmpty(equalFieldList)){
            throw new RuntimeException("no join condition found after table " + joinInfo.getLeftTableName());
        }

        for(String equalField : equalFieldList){
            if(fields.contains(equalField)){
                continue;
            }

            fields.add(equalField);
        }

        sideSelectFields = String.join(",", fields);
    }
}
