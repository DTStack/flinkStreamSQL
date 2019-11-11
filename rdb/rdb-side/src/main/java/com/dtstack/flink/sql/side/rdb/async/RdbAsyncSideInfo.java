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

package com.dtstack.flink.sql.side.rdb.async;

import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.SideInfo;
import com.dtstack.flink.sql.side.SideTableInfo;
import com.dtstack.flink.sql.side.rdb.table.RdbSideTableInfo;
import com.dtstack.flink.sql.util.ParseUtils;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.*;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;
import java.util.Map;


/**
 * Reason:
 * Date: 2018/11/26
 * Company: www.dtstack.com
 *
 * @author maqi
 */

public class RdbAsyncSideInfo extends SideInfo {

    private static final long serialVersionUID = 1942629132469918611L;

    public RdbAsyncSideInfo(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    @Override
    public void buildEqualInfo(JoinInfo joinInfo, SideTableInfo sideTableInfo) {
        RdbSideTableInfo rdbSideTableInfo = (RdbSideTableInfo) sideTableInfo;

        String sideTableName = joinInfo.getSideTableName();

        SqlNode conditionNode = joinInfo.getCondition();

        List<SqlNode> sqlNodeList = Lists.newArrayList();

        List<String> sqlJoinCompareOperate= Lists.newArrayList();

        ParseUtils.parseAnd(conditionNode, sqlNodeList);
        ParseUtils.parseJoinCompareOperate(conditionNode, sqlJoinCompareOperate);

        for (SqlNode sqlNode : sqlNodeList) {
            dealOneEqualCon(sqlNode, sideTableName);
        }

        List<String> whereConditionList = Lists.newArrayList();;
        Map<String, String> physicalFields = rdbSideTableInfo.getPhysicalFields();
        SqlNode whereNode = ((SqlSelect) joinInfo.getSelectNode()).getWhere();
        if (whereNode != null) {
            // 解析维表中的过滤条件
            ParseUtils.parseSideWhere(whereNode, physicalFields, whereConditionList);
        }

        sqlCondition = "select ${selectField} from ${tableName} where ";
        for (int i = 0; i < equalFieldList.size(); i++) {
            String equalField = sideTableInfo.getPhysicalFields().getOrDefault(equalFieldList.get(i), equalFieldList.get(i));

            sqlCondition += equalField + " " + sqlJoinCompareOperate.get(i) + " ? ";
            if (i != equalFieldList.size() - 1) {
                sqlCondition += " and ";
            }
        }
        if (0 != whereConditionList.size()) {
            // 如果where条件中第一个符合条件的是维表中的条件
            String firstCondition = whereConditionList.get(0);
            if (!"and".equalsIgnoreCase(firstCondition) && !"or".equalsIgnoreCase(firstCondition)) {
                whereConditionList.add(0, "and (");
            } else {
                whereConditionList.add(1, "(");
            }
            whereConditionList.add(whereConditionList.size(), ")");
            sqlCondition += String.join(" ", whereConditionList);
        }

        sqlCondition = sqlCondition.replace("${tableName}", rdbSideTableInfo.getTableName()).replace("${selectField}", sideSelectFields);

        System.out.println("--------side sql query:-------------------");
        System.out.println(sqlCondition);
    }


    @Override
    public void dealOneEqualCon(SqlNode sqlNode, String sideTableName) {
        if (!SqlKind.COMPARISON.contains(sqlNode.getKind())) {
            throw new RuntimeException("not compare operator.");
        }

        SqlIdentifier left = (SqlIdentifier) ((SqlBasicCall) sqlNode).getOperands()[0];
        SqlIdentifier right = (SqlIdentifier) ((SqlBasicCall) sqlNode).getOperands()[1];

        String leftTableName = left.getComponent(0).getSimple();
        String leftField = left.getComponent(1).getSimple();

        String rightTableName = right.getComponent(0).getSimple();
        String rightField = right.getComponent(1).getSimple();

        if (leftTableName.equalsIgnoreCase(sideTableName)) {
            equalFieldList.add(leftField);
            int equalFieldIndex = -1;
            for (int i = 0; i < rowTypeInfo.getFieldNames().length; i++) {
                String fieldName = rowTypeInfo.getFieldNames()[i];
                if (fieldName.equalsIgnoreCase(rightField)) {
                    equalFieldIndex = i;
                }
            }
            if (equalFieldIndex == -1) {
                throw new RuntimeException("can't deal equal field: " + sqlNode);
            }

            equalValIndex.add(equalFieldIndex);

        } else if (rightTableName.equalsIgnoreCase(sideTableName)) {

            equalFieldList.add(rightField);
            int equalFieldIndex = -1;
            for (int i = 0; i < rowTypeInfo.getFieldNames().length; i++) {
                String fieldName = rowTypeInfo.getFieldNames()[i];
                if (fieldName.equalsIgnoreCase(leftField)) {
                    equalFieldIndex = i;
                }
            }
            if (equalFieldIndex == -1) {
                throw new RuntimeException("can't deal equal field: " + sqlNode.toString());
            }

            equalValIndex.add(equalFieldIndex);

        } else {
            throw new RuntimeException("resolve equalFieldList error:" + sqlNode.toString());
        }

    }

}