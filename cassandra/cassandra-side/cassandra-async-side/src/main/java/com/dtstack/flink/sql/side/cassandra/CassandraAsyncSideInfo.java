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

package com.dtstack.flink.sql.side.cassandra;

import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.cassandra.table.CassandraSideTableInfo;
import com.dtstack.flink.sql.util.ParseUtils;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Reason:
 * Date: 2018/11/22
 *
 * @author xuqianjin
 */
public class CassandraAsyncSideInfo extends BaseSideInfo {

    private static final long serialVersionUID = -4403313049809013362L;
    private static final Logger LOG = LoggerFactory.getLogger(CassandraAsyncSideInfo.class.getSimpleName());


    public CassandraAsyncSideInfo(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, AbstractSideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    @Override
    public void buildEqualInfo(JoinInfo joinInfo, AbstractSideTableInfo sideTableInfo) {
        CassandraSideTableInfo cassandraSideTableInfo = (CassandraSideTableInfo) sideTableInfo;

        String sideTableName = joinInfo.getSideTableName();

        SqlNode conditionNode = joinInfo.getCondition();

        List<SqlNode> sqlNodeList = Lists.newArrayList();
        ParseUtils.parseAnd(conditionNode, sqlNodeList);

        for (SqlNode sqlNode : sqlNodeList) {
            dealOneEqualCon(sqlNode, sideTableName);
        }

        sqlCondition = "select ${selectField} from ${tableName}";
        sqlCondition = sqlCondition.replace("${tableName}", cassandraSideTableInfo.getDatabase()+"."+cassandraSideTableInfo.getTableName()).replace("${selectField}", sideSelectFields);

        LOG.info("---------side_exe_sql-----\n{}" + sqlCondition);
    }


    @Override
    public void dealOneEqualCon(SqlNode sqlNode, String sideTableName) {
        if (sqlNode.getKind() != SqlKind.EQUALS) {
            throw new RuntimeException("not equal operator.");
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
