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
import com.dtstack.flink.sql.side.FieldReplaceInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static org.apache.calcite.sql.SqlKind.*;
import static org.apache.calcite.sql.SqlKind.CASE;
import static org.apache.calcite.sql.SqlKind.OTHER;

/**
 * 表的解析相关
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


    /**
     * 获取节点关联的查询表
     * @param fromTable
     * @param tableNameSet
     */
    public static void getFromTableInfo(SqlNode fromTable, Set<String> tableNameSet){
        SqlKind sqlKind = fromTable.getKind();
        switch (sqlKind){
            case AS:
                SqlNode alias = ((SqlBasicCall) fromTable).getOperands()[1];
                tableNameSet.add(alias.toString());
                return;
            case JOIN:
                getFromTableInfo(((SqlJoin)fromTable).getLeft(), tableNameSet);
                getFromTableInfo(((SqlJoin)fromTable).getRight(), tableNameSet);
                return;
            case IDENTIFIER:
                tableNameSet.add(((SqlIdentifier)fromTable).getSimple());
                return;
            case SELECT:
                getFromTableInfo(((SqlSelect) fromTable).getFrom(), tableNameSet);
                return;
            default:
                throw new RuntimeException("not support sqlKind:" + sqlKind);
        }
    }

    public static void replaceSelectFieldTable(SqlNode selectNode, String oldTbName, String newTbName) {
        if (selectNode.getKind() == AS) {
            SqlNode leftNode = ((SqlBasicCall) selectNode).getOperands()[0];
            replaceSelectFieldTable(leftNode, oldTbName, newTbName);

        }else if(selectNode.getKind() == IDENTIFIER){
            SqlIdentifier sqlIdentifier = (SqlIdentifier) selectNode;

            if(sqlIdentifier.names.size() == 1){
                return ;
            }

            if(oldTbName.equalsIgnoreCase(((SqlIdentifier)selectNode).names.get(0))){
                SqlIdentifier newField = ((SqlIdentifier)selectNode).setName(0, newTbName);
                ((SqlIdentifier)selectNode).assignNamesFrom(newField);
            }

        }else if(selectNode.getKind() == LITERAL || selectNode.getKind() == LITERAL_CHAIN){//字面含义
            return;
        }else if(  AGGREGATE.contains(selectNode.getKind())
                || AVG_AGG_FUNCTIONS.contains(selectNode.getKind())
                || COMPARISON.contains(selectNode.getKind())
                || selectNode.getKind() == OTHER_FUNCTION
                || selectNode.getKind() == DIVIDE
                || selectNode.getKind() == CAST
                || selectNode.getKind() == TRIM
                || selectNode.getKind() == TIMES
                || selectNode.getKind() == PLUS
                || selectNode.getKind() == NOT_IN
                || selectNode.getKind() == OR
                || selectNode.getKind() == AND
                || selectNode.getKind() == MINUS
                || selectNode.getKind() == TUMBLE
                || selectNode.getKind() == TUMBLE_START
                || selectNode.getKind() == TUMBLE_END
                || selectNode.getKind() == SESSION
                || selectNode.getKind() == SESSION_START
                || selectNode.getKind() == SESSION_END
                || selectNode.getKind() == HOP
                || selectNode.getKind() == HOP_START
                || selectNode.getKind() == HOP_END
                || selectNode.getKind() == BETWEEN
                || selectNode.getKind() == IS_NULL
                || selectNode.getKind() == IS_NOT_NULL
                || selectNode.getKind() == CONTAINS
                || selectNode.getKind() == TIMESTAMP_ADD
                || selectNode.getKind() == TIMESTAMP_DIFF
                || selectNode.getKind() == LIKE

        ){
            SqlBasicCall sqlBasicCall = (SqlBasicCall) selectNode;
            for(int i=0; i<sqlBasicCall.getOperands().length; i++){
                SqlNode sqlNode = sqlBasicCall.getOperands()[i];
                if(sqlNode instanceof SqlLiteral){
                    continue;
                }

                if(sqlNode instanceof SqlDataTypeSpec){
                    continue;
                }

                replaceSelectFieldTable(sqlNode, oldTbName, newTbName);
            }

        }else if(selectNode.getKind() == CASE){
            SqlCase sqlCase = (SqlCase) selectNode;
            SqlNodeList whenOperands = sqlCase.getWhenOperands();
            SqlNodeList thenOperands = sqlCase.getThenOperands();
            SqlNode elseNode = sqlCase.getElseOperand();

            for(int i=0; i<whenOperands.size(); i++){
                SqlNode oneOperand = whenOperands.get(i);
                replaceSelectFieldTable(oneOperand, oldTbName, newTbName);
            }

            for(int i=0; i<thenOperands.size(); i++){
                SqlNode oneOperand = thenOperands.get(i);
                replaceSelectFieldTable(oneOperand, oldTbName, newTbName);

            }

            replaceSelectFieldTable(elseNode, oldTbName, newTbName);
        }else if(selectNode.getKind() == OTHER){
            //不处理
            return;
        }else{
            throw new RuntimeException(String.format("not support node kind of %s to replace name now.", selectNode.getKind()));
        }
    }

    /**
     * 替换另外join 表的指定表名为新关联处理的表名称
     * @param condition
     * @param tableRef
     */
    public static void replaceJoinFieldRefTableName(SqlNode condition, Map<String, String> tableRef){
        SqlKind joinKind = condition.getKind();
        if( joinKind == AND || joinKind == EQUALS ){
            replaceJoinFieldRefTableName(((SqlBasicCall)condition).operands[0], tableRef);
            replaceJoinFieldRefTableName(((SqlBasicCall)condition).operands[1], tableRef);
        }else{
            Preconditions.checkState(((SqlIdentifier)condition).names.size() == 2, "join condition must be format table.field");
            String fieldRefTable = ((SqlIdentifier)condition).names.get(0);

            String targetTableName = TableUtils.getTargetRefTable(tableRef, fieldRefTable);
            if(StringUtils.isNotBlank(targetTableName) && !fieldRefTable.equalsIgnoreCase(targetTableName)){
                SqlIdentifier newField = ((SqlIdentifier)condition).setName(0, targetTableName);
                ((SqlIdentifier)condition).assignNamesFrom(newField);
            }
        }
    }

    public static String getTargetRefTable(Map<String, String> refTableMap, String tableName){
        String targetTableName = null;
        String preTableName;

        do {
            preTableName = targetTableName == null ? tableName : targetTableName;
            targetTableName = refTableMap.get(preTableName);
        } while (targetTableName != null);

        return preTableName;
    }

    public static void replaceWhereCondition(SqlNode parentWhere, String oldTbName, String newTbName){

        if(parentWhere == null){
            return;
        }

        SqlKind kind = parentWhere.getKind();
        if(kind == AND){
            replaceWhereCondition(((SqlBasicCall) parentWhere).getOperands()[0], oldTbName, newTbName);
            replaceWhereCondition(((SqlBasicCall) parentWhere).getOperands()[1], oldTbName, newTbName);

        } else {
            replaceConditionNode(parentWhere, oldTbName, newTbName);
        }
    }

    private static void replaceConditionNode(SqlNode selectNode, String oldTbName, String newTbName) {
        if(selectNode.getKind() == IDENTIFIER){
            SqlIdentifier sqlIdentifier = (SqlIdentifier) selectNode;

            if(sqlIdentifier.names.size() == 1){
                return;
            }

            String tableName = sqlIdentifier.names.asList().get(0);
            if(tableName.equalsIgnoreCase(oldTbName)){
                SqlIdentifier newField = ((SqlIdentifier)selectNode).setName(0, newTbName);
                ((SqlIdentifier)selectNode).assignNamesFrom(newField);
            }
            return;
        }else if(selectNode.getKind() == LITERAL || selectNode.getKind() == LITERAL_CHAIN){//字面含义
            return;
        }else if(  AGGREGATE.contains(selectNode.getKind())
                || AVG_AGG_FUNCTIONS.contains(selectNode.getKind())
                || COMPARISON.contains(selectNode.getKind())
                || selectNode.getKind() == OTHER_FUNCTION
                || selectNode.getKind() == DIVIDE
                || selectNode.getKind() == CAST
                || selectNode.getKind() == TRIM
                || selectNode.getKind() == TIMES
                || selectNode.getKind() == PLUS
                || selectNode.getKind() == NOT_IN
                || selectNode.getKind() == OR
                || selectNode.getKind() == AND
                || selectNode.getKind() == MINUS
                || selectNode.getKind() == TUMBLE
                || selectNode.getKind() == TUMBLE_START
                || selectNode.getKind() == TUMBLE_END
                || selectNode.getKind() == SESSION
                || selectNode.getKind() == SESSION_START
                || selectNode.getKind() == SESSION_END
                || selectNode.getKind() == HOP
                || selectNode.getKind() == HOP_START
                || selectNode.getKind() == HOP_END
                || selectNode.getKind() == BETWEEN
                || selectNode.getKind() == IS_NULL
                || selectNode.getKind() == IS_NOT_NULL
                || selectNode.getKind() == CONTAINS
                || selectNode.getKind() == TIMESTAMP_ADD
                || selectNode.getKind() == TIMESTAMP_DIFF
                || selectNode.getKind() == LIKE

        ){
            SqlBasicCall sqlBasicCall = (SqlBasicCall) selectNode;
            for(int i=0; i<sqlBasicCall.getOperands().length; i++){
                SqlNode sqlNode = sqlBasicCall.getOperands()[i];
                if(sqlNode instanceof SqlLiteral){
                    continue;
                }

                if(sqlNode instanceof SqlDataTypeSpec){
                    continue;
                }

                replaceConditionNode(sqlNode, oldTbName, newTbName);
            }

            return;
        }else if(selectNode.getKind() == OTHER){
            //不处理
            return;
        }else{
            throw new RuntimeException(String.format("not support node kind of %s to replace name now.", selectNode.getKind()));
        }
    }

    /**
     * 获取条件中关联的表信息
     * @param selectNode
     * @param tableNameSet
     */
    public static void getConditionRefTable(SqlNode selectNode, Set<String> tableNameSet) {
        if(selectNode.getKind() == IDENTIFIER){
            SqlIdentifier sqlIdentifier = (SqlIdentifier) selectNode;

            if(sqlIdentifier.names.size() == 1){
                return;
            }

            String tableName = sqlIdentifier.names.asList().get(0);
            tableNameSet.add(tableName);
            return;
        }else if(selectNode.getKind() == LITERAL || selectNode.getKind() == LITERAL_CHAIN){//字面含义
            return;
        }else if(  AGGREGATE.contains(selectNode.getKind())
                || AVG_AGG_FUNCTIONS.contains(selectNode.getKind())
                || COMPARISON.contains(selectNode.getKind())
                || selectNode.getKind() == OTHER_FUNCTION
                || selectNode.getKind() == DIVIDE
                || selectNode.getKind() == CAST
                || selectNode.getKind() == TRIM
                || selectNode.getKind() == TIMES
                || selectNode.getKind() == PLUS
                || selectNode.getKind() == NOT_IN
                || selectNode.getKind() == OR
                || selectNode.getKind() == AND
                || selectNode.getKind() == MINUS
                || selectNode.getKind() == TUMBLE
                || selectNode.getKind() == TUMBLE_START
                || selectNode.getKind() == TUMBLE_END
                || selectNode.getKind() == SESSION
                || selectNode.getKind() == SESSION_START
                || selectNode.getKind() == SESSION_END
                || selectNode.getKind() == HOP
                || selectNode.getKind() == HOP_START
                || selectNode.getKind() == HOP_END
                || selectNode.getKind() == BETWEEN
                || selectNode.getKind() == IS_NULL
                || selectNode.getKind() == IS_NOT_NULL
                || selectNode.getKind() == CONTAINS
                || selectNode.getKind() == TIMESTAMP_ADD
                || selectNode.getKind() == TIMESTAMP_DIFF
                || selectNode.getKind() == LIKE

        ){
            SqlBasicCall sqlBasicCall = (SqlBasicCall) selectNode;
            for(int i=0; i<sqlBasicCall.getOperands().length; i++){
                SqlNode sqlNode = sqlBasicCall.getOperands()[i];
                if(sqlNode instanceof SqlLiteral){
                    continue;
                }

                if(sqlNode instanceof SqlDataTypeSpec){
                    continue;
                }

                getConditionRefTable(sqlNode, tableNameSet);
            }

            return;
        }else if(selectNode.getKind() == OTHER){
            //不处理
            return;
        }else{
            throw new RuntimeException(String.format("not support node kind of %s to replace name now.", selectNode.getKind()));
        }
    }
}
