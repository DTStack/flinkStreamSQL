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
import com.dtstack.flink.sql.side.JoinInfo;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.api.Table;

import java.util.List;
import java.util.Map;
import java.util.Queue;

import static org.apache.calcite.sql.SqlKind.SELECT;

/**
 *
 * Date: 2020/2/17
 * Company: www.dtstack.com
 * @author xuchao
 */

public class TableUtils {

    public static final char SPLIT = '_';

    /**
     * 获取select 的字段
     * @param sqlSelect
     */
    public static List<FieldInfo> parserSelectField(SqlSelect sqlSelect, Map<String, Table> localTableCache){
        SqlNodeList sqlNodeList = sqlSelect.getSelectList();
        List<FieldInfo> fieldInfoList = Lists.newArrayList();
        String fromNode = sqlSelect.getFrom().toString();

        for(SqlNode fieldNode : sqlNodeList.getList()){
            SqlIdentifier identifier = (SqlIdentifier)fieldNode;
            if(!identifier.isStar()) {
                String tableName = identifier.names.size() == 1 ? fromNode : identifier.getComponent(0).getSimple();
                String fieldName = identifier.names.size() == 1 ? identifier.getComponent(0).getSimple() : identifier.getComponent(1).getSimple();
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

    public static String buildInternalTableName(String left, char split, String right) {
        StringBuilder sb = new StringBuilder();
        return sb.append(left).append(split).append(right).toString();
    }

    public static SqlBasicCall buildAsNodeByJoinInfo(JoinInfo joinInfo, SqlNode sqlNode0, String tableAlias) {
        SqlOperator operator = new SqlAsOperator();

        SqlParserPos sqlParserPos = new SqlParserPos(0, 0);
        String joinLeftTableName = joinInfo.getLeftTableName();
        String joinLeftTableAlias = joinInfo.getLeftTableAlias();
        joinLeftTableName = Strings.isNullOrEmpty(joinLeftTableName) ? joinLeftTableAlias : joinLeftTableName;
        String newTableName = buildInternalTableName(joinLeftTableName, SPLIT, joinInfo.getRightTableName());
        String newTableAlias = !StringUtils.isEmpty(tableAlias) ? tableAlias : buildInternalTableName(joinInfo.getLeftTableAlias(), SPLIT, joinInfo.getRightTableAlias());

        if (null == sqlNode0) {
            sqlNode0 = new SqlIdentifier(newTableName, null, sqlParserPos);
        }

        SqlIdentifier sqlIdentifierAlias = new SqlIdentifier(newTableAlias, null, sqlParserPos);
        SqlNode[] sqlNodes = new SqlNode[2];
        sqlNodes[0] = sqlNode0;
        sqlNodes[1] = sqlIdentifierAlias;
        return new SqlBasicCall(operator, sqlNodes, sqlParserPos);
    }

    /**
     *
     * @param joinInfo
     * @param sqlNode
     * @param queueInfo
     * @return   两个边关联后的新表表名
     */
    public static String dealSelectResultWithJoinInfo(JoinInfo joinInfo, SqlSelect sqlNode, Queue<Object> queueInfo) {
        //SideJoinInfo rename
        if (joinInfo.checkIsSide()) {
            joinInfo.setSelectFields(sqlNode.getSelectList());
            joinInfo.setSelectNode(sqlNode);
            if (joinInfo.isRightIsSideTable()) {
                //Analyzing left is not a simple table
                if (joinInfo.getLeftNode().getKind() == SELECT) {
                    queueInfo.offer(joinInfo.getLeftNode());
                }

                queueInfo.offer(joinInfo);
            } else {
                //Determining right is not a simple table
                if (joinInfo.getRightNode().getKind() == SELECT) {
                    queueInfo.offer(joinInfo.getLeftNode());
                }

                queueInfo.offer(joinInfo);
            }
            replaceFromNodeForJoin(joinInfo, sqlNode);
            return joinInfo.getNewTableName();
        }
        return "";
    }

    public static void replaceFromNodeForJoin(JoinInfo joinInfo, SqlSelect sqlNode) {
        //Update from node
        SqlBasicCall sqlBasicCall = buildAsNodeByJoinInfo(joinInfo, null, null);
        sqlNode.setFrom(sqlBasicCall);
    }

}
