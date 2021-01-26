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

import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.PredicateInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.api.Table;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.calcite.sql.SqlKind.AGGREGATE;
import static org.apache.calcite.sql.SqlKind.AND;
import static org.apache.calcite.sql.SqlKind.AS;
import static org.apache.calcite.sql.SqlKind.AVG_AGG_FUNCTIONS;
import static org.apache.calcite.sql.SqlKind.BETWEEN;
import static org.apache.calcite.sql.SqlKind.CASE;
import static org.apache.calcite.sql.SqlKind.CAST;
import static org.apache.calcite.sql.SqlKind.COALESCE;
import static org.apache.calcite.sql.SqlKind.COMPARISON;
import static org.apache.calcite.sql.SqlKind.CONTAINS;
import static org.apache.calcite.sql.SqlKind.DIVIDE;
import static org.apache.calcite.sql.SqlKind.EQUALS;
import static org.apache.calcite.sql.SqlKind.HOP;
import static org.apache.calcite.sql.SqlKind.HOP_END;
import static org.apache.calcite.sql.SqlKind.HOP_START;
import static org.apache.calcite.sql.SqlKind.IDENTIFIER;
import static org.apache.calcite.sql.SqlKind.IS_NOT_NULL;
import static org.apache.calcite.sql.SqlKind.IS_NULL;
import static org.apache.calcite.sql.SqlKind.LIKE;
import static org.apache.calcite.sql.SqlKind.LITERAL;
import static org.apache.calcite.sql.SqlKind.LITERAL_CHAIN;
import static org.apache.calcite.sql.SqlKind.MINUS;
import static org.apache.calcite.sql.SqlKind.NOT_IN;
import static org.apache.calcite.sql.SqlKind.OR;
import static org.apache.calcite.sql.SqlKind.OTHER;
import static org.apache.calcite.sql.SqlKind.OTHER_FUNCTION;
import static org.apache.calcite.sql.SqlKind.PLUS;
import static org.apache.calcite.sql.SqlKind.SELECT;
import static org.apache.calcite.sql.SqlKind.SESSION;
import static org.apache.calcite.sql.SqlKind.SESSION_END;
import static org.apache.calcite.sql.SqlKind.SESSION_START;
import static org.apache.calcite.sql.SqlKind.TIMES;
import static org.apache.calcite.sql.SqlKind.TIMESTAMP_ADD;
import static org.apache.calcite.sql.SqlKind.TIMESTAMP_DIFF;
import static org.apache.calcite.sql.SqlKind.TRIM;
import static org.apache.calcite.sql.SqlKind.TUMBLE;
import static org.apache.calcite.sql.SqlKind.TUMBLE_END;
import static org.apache.calcite.sql.SqlKind.TUMBLE_START;
import static org.apache.calcite.sql.SqlKind.UNION;

/**
 * 表的解析相关
 * Date: 2020/2/17
 * Company: www.dtstack.com
 * @author xuchao
 */

public class TableUtils {

    public static final char SPLIT = '_';
    public static final Pattern stringPattern = Pattern.compile("\".*?\"|\'.*?\'");
    /**
     * 获取select 的字段
     * @param sqlSelect
     */
    public static List<FieldInfo> parserSelectField(SqlSelect sqlSelect, Map<String, Table> localTableCache){
        SqlNodeList sqlNodeList = sqlSelect.getSelectList();
        List<FieldInfo> fieldInfoList = Lists.newArrayList();
        String fromNode = sqlSelect.getFrom().toString();

        for (SqlNode fieldNode : sqlNodeList.getList()) {
            extractSelectFieldToFieldInfo(fieldNode,fromNode,fieldInfoList,localTableCache);
        }

        return fieldInfoList;
    }

    /**
     *  解析select Node 提取FieldInfo
     * @param fieldNode
     * @param fromNode
     * @param fieldInfoList
     * @param localTableCache
     */
    public static void extractSelectFieldToFieldInfo(SqlNode fieldNode, String fromNode, List<FieldInfo> fieldInfoList, Map<String, Table> localTableCache) {
        if (fieldNode.getKind() == IDENTIFIER) {
            SqlIdentifier identifier = (SqlIdentifier) fieldNode;
            if (!identifier.isStar()) {
                String tableName = identifier.names.size() == 1 ? fromNode : identifier.getComponent(0).getSimple();
                String fieldName = identifier.names.size() == 1 ? identifier.getComponent(0).getSimple() : identifier.getComponent(1).getSimple();
                FieldInfo fieldInfo = new FieldInfo();
                fieldInfo.setTable(tableName);
                fieldInfo.setFieldName(fieldName);

                if (!fieldInfoList.contains(fieldInfo)) {
                    fieldInfoList.add(fieldInfo);
                }
            } else {
                //处理
                int identifierSize = identifier.names.size();
                switch (identifierSize) {
                    case 1:
                        throw new RuntimeException("not support to parse * without scope of table");
                    default:
                        SqlIdentifier tableIdentify = identifier.skipLast(1);
                        Table registerTable = localTableCache.get(tableIdentify.getSimple());
                        if (registerTable == null) {
                            throw new RuntimeException("can't find table alias " + tableIdentify.getSimple());
                        }

                        String[] fieldNames = registerTable.getSchema().getFieldNames();
                        for (String fieldName : fieldNames) {
                            FieldInfo fieldInfo = new FieldInfo();
                            fieldInfo.setTable(tableIdentify.getSimple());
                            fieldInfo.setFieldName(fieldName);
                            fieldInfoList.add(fieldInfo);
                        }
                }
            }
        } else if (AGGREGATE.contains(fieldNode.getKind())
                || AVG_AGG_FUNCTIONS.contains(fieldNode.getKind())
                || COMPARISON.contains(fieldNode.getKind())
                || fieldNode.getKind() == OTHER_FUNCTION
                || fieldNode.getKind() == DIVIDE
                || fieldNode.getKind() == CAST
                || fieldNode.getKind() == TRIM
                || fieldNode.getKind() == TIMES
                || fieldNode.getKind() == PLUS
                || fieldNode.getKind() == NOT_IN
                || fieldNode.getKind() == OR
                || fieldNode.getKind() == AND
                || fieldNode.getKind() == MINUS
                || fieldNode.getKind() == TUMBLE
                || fieldNode.getKind() == TUMBLE_START
                || fieldNode.getKind() == TUMBLE_END
                || fieldNode.getKind() == SESSION
                || fieldNode.getKind() == SESSION_START
                || fieldNode.getKind() == SESSION_END
                || fieldNode.getKind() == HOP
                || fieldNode.getKind() == HOP_START
                || fieldNode.getKind() == HOP_END
                || fieldNode.getKind() == BETWEEN
                || fieldNode.getKind() == IS_NULL
                || fieldNode.getKind() == IS_NOT_NULL
                || fieldNode.getKind() == CONTAINS
                || fieldNode.getKind() == TIMESTAMP_ADD
                || fieldNode.getKind() == TIMESTAMP_DIFF
                || fieldNode.getKind() == LIKE
                || fieldNode.getKind() == COALESCE
                ) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) fieldNode;
            for (int i = 0; i < sqlBasicCall.getOperands().length; i++) {
                SqlNode sqlNode = sqlBasicCall.getOperands()[i];
                if (sqlNode instanceof SqlLiteral) {
                    continue;
                }

                if (sqlNode instanceof SqlDataTypeSpec) {
                    continue;
                }
                extractSelectFieldToFieldInfo(sqlNode, fromNode, fieldInfoList, localTableCache);
            }
        } else if (fieldNode.getKind() == AS) {
            SqlNode leftNode = ((SqlBasicCall) fieldNode).getOperands()[0];
            extractSelectFieldToFieldInfo(leftNode, fromNode,fieldInfoList, localTableCache);
        } else if (fieldNode.getKind() == CASE) {
            SqlCase sqlCase = (SqlCase) fieldNode;
            SqlNodeList whenOperands = sqlCase.getWhenOperands();
            SqlNodeList thenOperands = sqlCase.getThenOperands();
            SqlNode elseNode = sqlCase.getElseOperand();

            for (int i = 0; i < whenOperands.size(); i++) {
                SqlNode oneOperand = whenOperands.get(i);
                extractSelectFieldToFieldInfo(oneOperand, fromNode, fieldInfoList, localTableCache);
            }

            for (int i = 0; i < thenOperands.size(); i++) {
                SqlNode oneOperand = thenOperands.get(i);
                extractSelectFieldToFieldInfo(oneOperand, fromNode, fieldInfoList, localTableCache);

            }

            extractSelectFieldToFieldInfo(elseNode, fromNode, fieldInfoList, localTableCache);
        }
    }

    public static SqlBasicCall buildAsNodeByJoinInfo(JoinInfo joinInfo, SqlNode sqlNode0, String tableAlias) {
        SqlOperator operator = new SqlAsOperator();

        SqlParserPos sqlParserPos = new SqlParserPos(0, 0);
        String newTableName = joinInfo.getNewTableName();

        String newTableAlias = !StringUtils.isEmpty(tableAlias) ? tableAlias : joinInfo.getNewTableAlias();

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

                if(joinInfo.getLeftNode().getKind() == AS){
                    SqlNode leftSqlNode = ((SqlBasicCall)joinInfo.getLeftNode()).getOperands()[0];
                    if (leftSqlNode.getKind() == UNION){
                        queueInfo.offer(joinInfo.getLeftNode());
                    }
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
        String newAliasName = sqlBasicCall.operand(1).toString();

        //替换select 中的属性为新的表名称和字段
        HashBasedTable<String, String, String> fieldMapping = joinInfo.getTableFieldRef();
        Map<String, String> leftFieldMapping = fieldMapping.row(joinInfo.getLeftTableAlias());
        Map<String, String> rightFieldMapping = fieldMapping.row(joinInfo.getRightTableAlias());

       /* for(SqlNode oneSelectNode : sqlNode.getSelectList()){
            replaceSelectFieldTable(oneSelectNode, joinInfo.getLeftTableAlias(), newAliasName, null ,leftFieldMapping);
            replaceSelectFieldTable(oneSelectNode, joinInfo.getRightTableAlias(), newAliasName, null , rightFieldMapping);
        }*/

        //where中的条件属性为新的表名称和字段
        FieldReplaceUtil.replaceFieldName(sqlNode, joinInfo.getLeftTableAlias(), newAliasName, leftFieldMapping);
        FieldReplaceUtil.replaceFieldName(sqlNode, joinInfo.getRightTableAlias(), newAliasName, rightFieldMapping);
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

    /**
     * 替换select 中的字段信息
     * 如果mappingTable 非空则从该参数获取字段的映射
     * 如果mappingTable 为空则根据是否存在新生成字段
     * @param selectNode
     * @param oldTbName
     * @param newTbName
     * @param fieldReplaceRef
     */
    public static void replaceSelectFieldTable(SqlNode selectNode,
                                               String oldTbName,
                                               String newTbName,
                                               Map<String, String> fieldReplaceRef) {
        if (selectNode.getKind() == AS) {
            SqlNode leftNode = ((SqlBasicCall) selectNode).getOperands()[0];
            replaceSelectFieldTable(leftNode, oldTbName, newTbName, fieldReplaceRef);

        }else if(selectNode.getKind() == IDENTIFIER){
            SqlIdentifier sqlIdentifier = (SqlIdentifier) selectNode;

            if(sqlIdentifier.names.size() == 1){
                return ;
            }

            String fieldTableName = sqlIdentifier.names.get(0);
            if(oldTbName.equalsIgnoreCase(fieldTableName)){
                replaceOneSelectField(sqlIdentifier, newTbName, oldTbName, fieldReplaceRef);
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
                || selectNode.getKind() == COALESCE

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

                replaceSelectFieldTable(sqlNode, oldTbName, newTbName, fieldReplaceRef);
            }

        }else if(selectNode.getKind() == CASE){
            SqlCase sqlCase = (SqlCase) selectNode;
            SqlNodeList whenOperands = sqlCase.getWhenOperands();
            SqlNodeList thenOperands = sqlCase.getThenOperands();
            SqlNode elseNode = sqlCase.getElseOperand();

            for(int i=0; i<whenOperands.size(); i++){
                SqlNode oneOperand = whenOperands.get(i);
                replaceSelectFieldTable(oneOperand, oldTbName, newTbName, fieldReplaceRef);
            }

            for(int i=0; i<thenOperands.size(); i++){
                SqlNode oneOperand = thenOperands.get(i);
                replaceSelectFieldTable(oneOperand, oldTbName, newTbName, fieldReplaceRef);

            }

            replaceSelectFieldTable(elseNode, oldTbName, newTbName, fieldReplaceRef);
        }else if(selectNode.getKind() == OTHER){
            //不处理
            return;
        }else{
            throw new RuntimeException(String.format("not support node kind of %s to replace name now.", selectNode.getKind()));
        }
    }

    private static void replaceOneSelectField(SqlIdentifier sqlIdentifier,
                                              String newTbName,
                                              String oldTbName,
                                              Map<String, String> fieldReplaceRef){
        SqlIdentifier newField = sqlIdentifier.setName(0, newTbName);
        String fieldName = sqlIdentifier.names.get(1);
        String fieldKey = oldTbName + "." + fieldName;
        if(fieldReplaceRef.get(fieldKey) != null){
            String newFieldName = fieldReplaceRef.get(fieldKey).split("\\.")[1];
            newField = newField.setName(1, newFieldName);
        }

        sqlIdentifier.assignNamesFrom(newField);
    }

    /**
     * 替换另外join 表的指定表名为新关联处理的表名称
     * @param condition
     * @param oldTabFieldRefNew
     */
    public static void replaceJoinFieldRefTableName(SqlNode condition, Map<String, String> oldTabFieldRefNew){
        if (null == condition || condition.getKind() == LITERAL) {
            return;
        }
        SqlKind joinKind = condition.getKind();
        if( AGGREGATE.contains(joinKind)
                || AVG_AGG_FUNCTIONS.contains(joinKind)
                || COMPARISON.contains(joinKind)
                || joinKind == OTHER_FUNCTION
                || joinKind == DIVIDE
                || joinKind == CAST
                || joinKind == TRIM
                || joinKind == TIMES
                || joinKind == PLUS
                || joinKind == NOT_IN
                || joinKind == OR
                || joinKind == AND
                || joinKind == MINUS
                || joinKind == TUMBLE
                || joinKind == TUMBLE_START
                || joinKind == TUMBLE_END
                || joinKind == SESSION
                || joinKind == SESSION_START
                || joinKind == SESSION_END
                || joinKind == HOP
                || joinKind == HOP_START
                || joinKind == HOP_END
                || joinKind == BETWEEN
                || joinKind == IS_NULL
                || joinKind == IS_NOT_NULL
                || joinKind == CONTAINS
                || joinKind == TIMESTAMP_ADD
                || joinKind == TIMESTAMP_DIFF
                || joinKind == LIKE
                || joinKind == COALESCE
                || joinKind == EQUALS ){

            SqlBasicCall sqlBasicCall = (SqlBasicCall) condition;
            for(int i=0; i<sqlBasicCall.getOperands().length; i++){
                SqlNode sqlNode = sqlBasicCall.getOperands()[i];
                if(sqlNode instanceof SqlLiteral){
                    continue;
                }

                if(sqlNode instanceof SqlDataTypeSpec){
                    continue;
                }

                replaceJoinFieldRefTableName(sqlNode, oldTabFieldRefNew);
            }

        } else if (condition.getKind() == IDENTIFIER) {
            Preconditions.checkState(((SqlIdentifier)condition).names.size() == 2, "join condition must be format table.field");
            String fieldRefTable = ((SqlIdentifier)condition).names.get(0);

            String targetFieldName = TableUtils.getTargetRefField(oldTabFieldRefNew, condition.toString());

            if(StringUtils.isNotBlank(targetFieldName)){
                String[] fieldSplits = StringUtils.split(targetFieldName, ".");
                SqlIdentifier newField = ((SqlIdentifier)condition).setName(0, fieldSplits[0]);
                newField = newField.setName(1, fieldSplits[1]);
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

    public static String getTargetRefField(Map<String, String> refFieldMap, String currFieldName){
        String targetFieldName = null;
        String preFieldName;

        do {
            preFieldName = targetFieldName == null ? currFieldName : targetFieldName;
            targetFieldName = refFieldMap.get(preFieldName);
        } while (targetFieldName != null);

        return preFieldName;
    }

    public static void replaceWhereCondition(SqlNode parentWhere, String oldTbName, String newTbName, Map<String, String> fieldReplaceRef){

        if(parentWhere == null){
            return;
        }

        SqlKind kind = parentWhere.getKind();
        if(kind == AND){
            replaceWhereCondition(((SqlBasicCall) parentWhere).getOperands()[0], oldTbName, newTbName, fieldReplaceRef);
            replaceWhereCondition(((SqlBasicCall) parentWhere).getOperands()[1], oldTbName, newTbName, fieldReplaceRef);

        } else {
            replaceConditionNode(parentWhere, oldTbName, newTbName, fieldReplaceRef);
        }
    }

    private static void replaceConditionNode(SqlNode selectNode, String oldTbName, String newTbName, Map<String, String> fieldReplaceRef) {
        if(selectNode.getKind() == IDENTIFIER){
            SqlIdentifier sqlIdentifier = (SqlIdentifier) selectNode;

            if(sqlIdentifier.names.size() == 1){
                return;
            }

            String tableName = sqlIdentifier.names.asList().get(0);
            String tableField = sqlIdentifier.names.asList().get(1);
            String fieldKey = tableName + "_" + tableField;

            if(tableName.equalsIgnoreCase(oldTbName)){

                String newFieldName = fieldReplaceRef.get(fieldKey) == null ? tableField : fieldReplaceRef.get(fieldKey);
                SqlIdentifier newField = ((SqlIdentifier)selectNode).setName(0, newTbName);
                newField = newField.setName(1, newFieldName);
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
                || selectNode.getKind() == COALESCE

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

                replaceConditionNode(sqlNode, oldTbName, newTbName, fieldReplaceRef);
            }

            return;
        }else if(selectNode.getKind() == OTHER){
            //不处理
            return;
        } else if (selectNode.getKind() == CASE) {
            SqlCase sqlCase = (SqlCase) selectNode;

            sqlCase.getWhenOperands().getList().forEach(sqlNode -> replaceConditionNode(sqlNode, oldTbName, newTbName, fieldReplaceRef));
            sqlCase.getThenOperands().getList().forEach(sqlNode -> replaceConditionNode(sqlNode, oldTbName, newTbName, fieldReplaceRef));
            replaceConditionNode(sqlCase.getElseOperand(), oldTbName, newTbName, fieldReplaceRef);
        } else {
            throw new RuntimeException(String.format("not support node kind of %s to replace name now.", selectNode.getKind()));
        }
    }

    /**
     * 获取条件中关联的表信息
     * @param selectNode
     * @param fieldInfos
     */
    public static void getConditionRefTable(SqlNode selectNode, Set<String> fieldInfos) {
        if (selectNode.getKind() == IDENTIFIER) {
            SqlIdentifier sqlIdentifier = (SqlIdentifier) selectNode;

            fieldInfos.add(sqlIdentifier.toString());
            return;
        } else if (selectNode.getKind() == LITERAL || selectNode.getKind() == LITERAL_CHAIN) {//字面含义
            return;
        } else if (AGGREGATE.contains(selectNode.getKind())
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
                || selectNode.getKind() == COALESCE

                ) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) selectNode;
            for (int i = 0; i < sqlBasicCall.getOperands().length; i++) {
                SqlNode sqlNode = sqlBasicCall.getOperands()[i];
                if (sqlNode instanceof SqlLiteral) {
                    continue;
                }

                if (sqlNode instanceof SqlDataTypeSpec) {
                    continue;
                }

                getConditionRefTable(sqlNode, fieldInfos);
            }

            return;
        } else if (selectNode.getKind() == OTHER) {
            //不处理
            return;
        } else if (selectNode.getKind() == CASE) {
            SqlCase sqlCase = (SqlCase) selectNode;

            sqlCase.getWhenOperands().getList().forEach(sqlNode -> getConditionRefTable(sqlNode, fieldInfos));
            sqlCase.getThenOperands().getList().forEach(sqlNode -> getConditionRefTable(sqlNode, fieldInfos));
            getConditionRefTable(sqlCase.getElseOperand(), fieldInfos);
        } else {
            throw new RuntimeException(String.format("not support node kind of %s to replace name now.", selectNode.getKind()));
        }
    }

    public static String buildTableField(String tableName, String fieldName){
        return String.format("%s.%s", tableName, fieldName);
    }


    public static String buildTableNameWithScope(String tableName, String scope){
        if(StringUtils.isEmpty(scope)){
            return tableName;
        }

        return tableName + "_" + scope;
    }

    /**
     * add constant join fields, using in such as hbase、redis etc kv database
     * @param keyMap
     */
    public static void addConstant(Map<String, Object> keyMap, AbstractSideTableInfo sideTableInfo) {
        List<PredicateInfo> predicateInfos = sideTableInfo.getPredicateInfoes();
        final String name = sideTableInfo.getName();
        for (PredicateInfo info : predicateInfos) {
            if (info.getOwnerTable().equals(name)
                && info.getOperatorName().equals("=")) {
                String condition = info.getCondition();
                Matcher matcher = stringPattern.matcher(condition);
                if (matcher.matches()) {
                    condition = condition.substring(1, condition.length() - 1);
                }
                keyMap.put(info.getFieldName(), condition);
            }
        }
    }
    public static String buildTableNameWithScope(String leftTableName, String leftTableAlias, String rightTableName, String scope, Set<String> existTableNames){
        //兼容左边表是as 的情况
        String leftStr = Strings.isNullOrEmpty(leftTableName) ? leftTableAlias : leftTableName;
        String newName = leftStr + "_" + rightTableName;
        if (CollectionUtils.isEmpty(existTableNames)) {
            return TableUtils.buildTableNameWithScope(newName, scope);
        }

        if (!existTableNames.contains(newName)) {
            return TableUtils.buildTableNameWithScope(newName, scope);
        }

        return TableUtils.buildTableNameWithScope(newName, scope) + "_" + System.currentTimeMillis();
    }

    /**
     * 判断目标查询是否是基于新构建出来的表的窗口group by
     * @param sqlNode
     * @param newRegisterTableList
     * @return
     */
    public static boolean checkIsGroupByTimeWindow(SqlNode sqlNode, Collection<String> newRegisterTableList) {
        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind) {
            case SELECT:
                SqlSelect selectNode = (SqlSelect) sqlNode;
                SqlNodeList groupNodeList = selectNode.getGroup();
                if ( groupNodeList == null || groupNodeList.size() == 0) {
                    return false;
                }

                SqlNode fromNode = selectNode.getFrom();
                if (fromNode.getKind() != IDENTIFIER
                    && fromNode.getKind() != AS) {
                    return false;
                }

                if(selectNode.getFrom().getKind() == AS){
                    SqlNode asNode = ((SqlBasicCall) selectNode.getFrom()).getOperands()[0];
                    if(asNode.getKind() != IDENTIFIER){
                        return false;
                    }

                    fromNode = asNode;
                }

                String tableName = fromNode.toString();
                for (SqlNode node : groupNodeList.getList()) {
                    if (node.getKind() == OTHER_FUNCTION) {
                        String functionName = ((SqlBasicCall) node).getOperator().toString().toLowerCase();
                        boolean isTimeGroupByFunction = checkIsTimeGroupByFunction(functionName);
                        if(isTimeGroupByFunction && newRegisterTableList.contains(tableName)){
                            return true;
                        }
                    }
                }

                return false;
            case INSERT:
                return checkIsGroupByTimeWindow(((SqlInsert) sqlNode).getSource(), newRegisterTableList);
            case UNION:
                SqlNode unionLeft = ((SqlBasicCall) sqlNode).getOperands()[0];
                SqlNode unionRight = ((SqlBasicCall) sqlNode).getOperands()[1];
                return checkIsGroupByTimeWindow(unionLeft, newRegisterTableList)
                        || checkIsGroupByTimeWindow(unionRight, newRegisterTableList);

            default:
                    return false;

        }
    }

    /**
     * 判断group by中是否包含维表，包含则需要撤回，不管嵌套多少层子查询只要有一层包含都需要撤回
     *
     * @param sqlNode              sql语句
     * @param newRegisterTableList 维表集合
     * @return true:需要撤回，false:和原生保持一样
     */
    public static boolean checkIsDimTableGroupBy(SqlNode sqlNode, Collection<String> newRegisterTableList) {
        // 维表集合为空
        if (newRegisterTableList == null || newRegisterTableList.size() == 0) {
            return false;
        }
        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind) {
            case SELECT:
                SqlSelect selectNode = (SqlSelect) sqlNode;
                SqlNodeList groupNodeList = selectNode.getGroup();
                SqlNode fromNode = selectNode.getFrom();

                // 1.(sub query) group by
                // 2.(sub query) as alias group by
                // 3.tableName group by
                // 4.tableName as alias group by
                // 5.others return false

                // (子查询) group by：1.(sub query) group by
                if (fromNode.getKind() == SELECT) {
                    return checkIsDimTableGroupBy(fromNode, newRegisterTableList);
                }

                // 表名 as 别名 group by、(子查询) as 别名 group by、表名 group by
                if (fromNode.getKind() == AS || fromNode.getKind() == IDENTIFIER) {
                    SqlNode operand;
                    // 表名 group by：3.tableName group by
                    if (fromNode.getKind() == IDENTIFIER) {
                        operand = fromNode;
                    } else {
                        // 表名 as 别名 group by：4.tableName as alias group by
                        operand = ((SqlBasicCall) fromNode).getOperands()[0];
                        // (子查询) as 别名 group by：2.(sub query) as alias group by
                        if (operand.getKind() != IDENTIFIER) {
                            return checkIsDimTableGroupBy(fromNode, newRegisterTableList);
                        }
                    }

                    // 最里层是表名 group by，且group by字段不为空，且表名包含在维表中
                    if (operand.getKind() == IDENTIFIER
                            && groupNodeList != null
                            && groupNodeList.size() != 0
                            && newRegisterTableList.contains(operand.toString())) {
                        return checkGroupByNode(groupNodeList);
                    }
                }

                return false;
            case INSERT:
                return checkIsDimTableGroupBy(((SqlInsert) sqlNode).getSource(), newRegisterTableList);
            case UNION:
                SqlNode unionLeft = ((SqlBasicCall) sqlNode).getOperands()[0];
                SqlNode unionRight = ((SqlBasicCall) sqlNode).getOperands()[1];
                return checkIsDimTableGroupBy(unionLeft, newRegisterTableList)
                        || checkIsDimTableGroupBy(unionRight, newRegisterTableList);
            case AS:
                SqlNode info = ((SqlBasicCall) sqlNode).getOperands()[0];
                return checkIsDimTableGroupBy(info, newRegisterTableList);
            default:
                return false;
        }
    }

    /**
     * 遍历每一个group by节点，判断是否需要撤回
     * @param groupNodeList
     * @return
     */
    private static boolean checkGroupByNode(SqlNodeList groupNodeList) {
        boolean isRetract = false;
        // 判断完所有的group by字段
        for (SqlNode node : groupNodeList.getList()) {
            // 判断是否有函数
            if (node.getKind() == OTHER_FUNCTION) {
                String functionName = ((SqlBasicCall) node).getOperator().toString().toLowerCase();
                boolean isTimeGroupByFunction = checkIsTimeGroupByFunction(functionName);
                // 只要有窗口就不需要撤回，直接返回
                if (isTimeGroupByFunction) {
                    return false;
                }
                // 非窗口需要撤回，继续迭代后面的字段
                isRetract = true;
            } else {
                // 其他情况需要撤回，继续迭代后面的字段
                isRetract = true;
            }
        }
        return isRetract;
    }

    public static boolean checkIsTimeGroupByFunction(String functionName ){
        return functionName.equalsIgnoreCase("tumble")
                || functionName.equalsIgnoreCase("session")
                || functionName.equalsIgnoreCase("hop");
    }
}
