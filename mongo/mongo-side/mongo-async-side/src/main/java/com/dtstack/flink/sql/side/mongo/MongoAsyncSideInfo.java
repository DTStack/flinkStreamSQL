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

package com.dtstack.flink.sql.side.mongo;

import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.SideInfo;
import com.dtstack.flink.sql.side.SideTableInfo;
import com.dtstack.flink.sql.side.mongo.table.MongoSideTableInfo;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;

import java.util.List;

/**
 * Reason:
 * Date: 2018/11/6
 *
 * @author xuqianjin
 */
public class MongoAsyncSideInfo extends SideInfo {

    private static final long serialVersionUID = -3694857194254465989L;

    public MongoAsyncSideInfo(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    @Override
    public void buildEqualInfo(JoinInfo joinInfo, SideTableInfo sideTableInfo) {
        MongoSideTableInfo MongoSideTableInfo = (MongoSideTableInfo) sideTableInfo;

        String sideTableName = joinInfo.getSideTableName();

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

        sqlCondition = "select ${selectField} from ${tableName} where ";
        for(int i=0; i<equalFieldList.size(); i++){
            String equalField = equalFieldList.get(i);

            sqlCondition += equalField + "=? ";
            if(i != equalFieldList.size() - 1){
                sqlCondition += " and ";
            }
        }

        sqlCondition = sqlCondition.replace("${tableName}", MongoSideTableInfo.getTableName()).replace("${selectField}", sideSelectFields);
        System.out.println("---------side_exe_sql-----\n" + sqlCondition);
    }


    @Override
    public void dealOneEqualCon(SqlNode sqlNode, String sideTableName){
        if(sqlNode.getKind() != SqlKind.EQUALS){
            throw new RuntimeException("not equal operator.");
        }

        SqlIdentifier left = (SqlIdentifier)((SqlBasicCall)sqlNode).getOperands()[0];
        SqlIdentifier right = (SqlIdentifier)((SqlBasicCall)sqlNode).getOperands()[1];

        String leftTableName = left.getComponent(0).getSimple();
        String leftField = left.getComponent(1).getSimple();

        String rightTableName = right.getComponent(0).getSimple();
        String rightField = right.getComponent(1).getSimple();

        if(leftTableName.equalsIgnoreCase(sideTableName)){
            equalFieldList.add(leftField);
            int equalFieldIndex = -1;
            for(int i=0; i<rowTypeInfo.getFieldNames().length; i++){
                String fieldName = rowTypeInfo.getFieldNames()[i];
                if(fieldName.equalsIgnoreCase(rightField)){
                    equalFieldIndex = i;
                }
            }
            if(equalFieldIndex == -1){
                throw new RuntimeException("can't deal equal field: " + sqlNode);
            }

            equalValIndex.add(equalFieldIndex);

        }else if(rightTableName.equalsIgnoreCase(sideTableName)){

            equalFieldList.add(rightField);
            int equalFieldIndex = -1;
            for(int i=0; i<rowTypeInfo.getFieldNames().length; i++){
                String fieldName = rowTypeInfo.getFieldNames()[i];
                if(fieldName.equalsIgnoreCase(leftField)){
                    equalFieldIndex = i;
                }
            }
            if(equalFieldIndex == -1){
                throw new RuntimeException("can't deal equal field: " + sqlNode.toString());
            }

            equalValIndex.add(equalFieldIndex);

        }else{
            throw new RuntimeException("resolve equalFieldList error:" + sqlNode.toString());
        }

    }

}
