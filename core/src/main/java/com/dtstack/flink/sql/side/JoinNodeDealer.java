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

import com.dtstack.flink.sql.config.CalciteConfig;
import com.dtstack.flink.sql.util.TableUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static org.apache.calcite.sql.SqlKind.*;

/**
 * 处理join 节点
 * 1:如果包含维表节点替换为临时查询
 * Date: 2020/2/27
 * Company: www.dtstack.com
 * @author xuchao
 */

public class JoinNodeDealer {

    //用来构建临时的中间查询
    private static final String SELECT_TEMP_SQL = "select %s from %s %s";

    private SideSQLParser sideSQLParser;

    public JoinNodeDealer(SideSQLParser sideSQLParser){
        this.sideSQLParser = sideSQLParser;
    }

    /**
     * 解析 join 操作
     * @param joinNode
     * @param sideTableSet 标明哪些表名是维表
     * @param queueInfo
     * @param parentWhere join 关联的最上层的where 节点
     * @param parentSelectList join 关联的最上层的select 节点
     * @param joinFieldSet
     * @param tableRef 存储构建临时表查询后源表和新表之间的关联关系
     * @return
     */
    public JoinInfo dealJoinNode(SqlJoin joinNode, Set<String> sideTableSet,
                                 Queue<Object> queueInfo, SqlNode parentWhere,
                                 SqlNodeList parentSelectList, Set<Tuple2<String, String>> joinFieldSet,
                                 Map<String, String> tableRef) {
        SqlNode leftNode = joinNode.getLeft();
        SqlNode rightNode = joinNode.getRight();
        JoinType joinType = joinNode.getJoinType();

        String leftTbName = "";
        String leftTbAlias = "";
        String rightTableName = "";
        String rightTableAlias = "";
        boolean leftTbisTmp = false;

        //如果是连续join 判断是否已经处理过添加到执行队列
        Boolean needBuildTemp = false;
        extractJoinField(joinNode.getCondition(), joinFieldSet);

        if(leftNode.getKind() == IDENTIFIER){
            leftTbName = leftNode.toString();
        } else if (leftNode.getKind() == JOIN) {
            //处理连续join
            Tuple2<Boolean, JoinInfo> nestJoinResult = dealNestJoin((SqlJoin) leftNode, sideTableSet,
                    queueInfo, parentWhere, parentSelectList, joinFieldSet, tableRef);
            needBuildTemp = nestJoinResult.f0;
            SqlBasicCall buildAs = TableUtils.buildAsNodeByJoinInfo(nestJoinResult.f1, null, null);

            if(needBuildTemp){
                //记录表之间的关联关系
                String newLeftTableName = buildAs.getOperands()[1].toString();
                Set<String> fromTableNameSet = Sets.newHashSet();
                TableUtils.getFromTableInfo(joinNode.getLeft(), fromTableNameSet);
                for(String tbTmp : fromTableNameSet){
                    tableRef.put(tbTmp, newLeftTableName);
                }

                //替换leftNode 为新的查询
                joinNode.setLeft(buildAs);
                leftNode = buildAs;

                //替换select field 中的对应字段
                for(SqlNode sqlNode : parentSelectList.getList()){
                    for(String tbTmp : fromTableNameSet) {
                        TableUtils.replaceSelectFieldTable(sqlNode, tbTmp, newLeftTableName);
                    }
                }

                //替换where 中的条件相关
                for(String tbTmp : fromTableNameSet){
                    TableUtils.replaceWhereCondition(parentWhere, tbTmp, newLeftTableName);
                }

                leftTbisTmp = true;

            }

            leftTbName = buildAs.getOperands()[0].toString();
            leftTbAlias = buildAs.getOperands()[1].toString();

        } else if (leftNode.getKind() == AS) {
            AliasInfo aliasInfo = (AliasInfo) sideSQLParser.parseSql(leftNode, sideTableSet, queueInfo, parentWhere, parentSelectList);
            leftTbName = aliasInfo.getName();
            leftTbAlias = aliasInfo.getAlias();

        } else {
            throw new RuntimeException(String.format("---not deal node with type %s", leftNode.getKind().toString()));
        }

        boolean leftIsSide = checkIsSideTable(leftTbName, sideTableSet);
        Preconditions.checkState(!leftIsSide, "side-table must be at the right of join operator");

        Tuple2<String, String> rightTableNameAndAlias = parseRightNode(rightNode, sideTableSet, queueInfo, parentWhere, parentSelectList);
        rightTableName = rightTableNameAndAlias.f0;
        rightTableAlias = rightTableNameAndAlias.f1;

        boolean rightIsSide = checkIsSideTable(rightTableName, sideTableSet);
        if(rightIsSide && joinType == JoinType.RIGHT){
            throw new RuntimeException("side join not support join type of right[current support inner join and left join]");
        }

        if(leftNode.getKind() == JOIN && rightIsSide){
            needBuildTemp = true;
        }

        JoinInfo tableInfo = new JoinInfo();
        tableInfo.setLeftTableName(leftTbName);
        tableInfo.setRightTableName(rightTableName);
        if (StringUtils.isEmpty(leftTbAlias)){
            tableInfo.setLeftTableAlias(leftTbName);
        } else {
            tableInfo.setLeftTableAlias(leftTbAlias);
        }

        if (StringUtils.isEmpty(rightTableAlias)){
            tableInfo.setRightTableAlias(rightTableName);
        } else {
            tableInfo.setRightTableAlias(rightTableAlias);
        }

        TableUtils.replaceJoinFieldRefTableName(joinNode.getCondition(), tableRef);

        tableInfo.setLeftIsTmpTable(leftTbisTmp);
        tableInfo.setLeftIsSideTable(leftIsSide);
        tableInfo.setRightIsSideTable(rightIsSide);
        tableInfo.setLeftNode(leftNode);
        tableInfo.setRightNode(rightNode);
        tableInfo.setJoinType(joinType);
        tableInfo.setCondition(joinNode.getCondition());

        if(tableInfo.getLeftNode().getKind() != AS && needBuildTemp){
            extractTemporaryQuery(tableInfo.getLeftNode(), tableInfo.getLeftTableAlias(), (SqlBasicCall) parentWhere,
                    parentSelectList, queueInfo, joinFieldSet, tableRef);
        }else {
            SqlKind asNodeFirstKind = ((SqlBasicCall)tableInfo.getLeftNode()).operands[0].getKind();
            if(asNodeFirstKind == SELECT){
                queueInfo.offer(tableInfo.getLeftNode());
                tableInfo.setLeftNode(((SqlBasicCall)tableInfo.getLeftNode()).operands[1]);
            }
        }
        return tableInfo;
    }


    //处理多层join
    private Tuple2<Boolean, JoinInfo> dealNestJoin(SqlJoin joinNode, Set<String> sideTableSet,
                                                   Queue<Object> queueInfo, SqlNode parentWhere,
                                                   SqlNodeList selectList, Set<Tuple2<String, String>> joinFieldSet,
                                                   Map<String, String> tableRef){
        SqlNode rightNode = joinNode.getRight();
        Tuple2<String, String> rightTableNameAndAlias = parseRightNode(rightNode, sideTableSet, queueInfo, parentWhere, selectList);
        JoinInfo joinInfo = dealJoinNode(joinNode, sideTableSet, queueInfo, parentWhere, selectList, joinFieldSet, tableRef);

        String rightTableName = rightTableNameAndAlias.f0;
        boolean rightIsSide = checkIsSideTable(rightTableName, sideTableSet);
        boolean needBuildTemp = false;

        if(!rightIsSide){
            //右表不是维表的情况
        }else{
            //右边表是维表需要重新构建左表的临时查询
            queueInfo.offer(joinInfo);
            needBuildTemp = true;
        }

        //return Tuple2.of(needBuildTemp, TableUtils.buildAsNodeByJoinInfo(joinInfo, null, null));
        return Tuple2.of(needBuildTemp, joinInfo);
    }

    private void extractTemporaryQuery(SqlNode node, String tableAlias, SqlBasicCall parentWhere,
                                       SqlNodeList parentSelectList, Queue<Object> queueInfo,
                                       Set<Tuple2<String, String>> joinFieldSet,
                                       Map<String, String> tableRef){
        try{
            //父一级的where 条件中如果只和临时查询相关的条件都截取进来
            Set<String> fromTableNameSet = Sets.newHashSet();
            List<SqlBasicCall> extractCondition = Lists.newArrayList();

            TableUtils.getFromTableInfo(node, fromTableNameSet);
            checkAndRemoveWhereCondition(fromTableNameSet, parentWhere, extractCondition);

            if(node.getKind() == JOIN){
                checkAndReplaceJoinCondition(((SqlJoin)node).getCondition(), tableRef);
            }

            Set<String> extractSelectField = extractSelectFields(parentSelectList, fromTableNameSet, tableRef);
            Set<String> fieldFromJoinCondition = extractSelectFieldFromJoinCondition(joinFieldSet, fromTableNameSet);
            String extractSelectFieldStr = buildSelectNode(extractSelectField, fieldFromJoinCondition);
            String extractConditionStr = buildCondition(extractCondition);

            String tmpSelectSql = String.format(SELECT_TEMP_SQL,
                    extractSelectFieldStr,
                    node.toString(),
                    extractConditionStr);

            SqlParser sqlParser = SqlParser.create(tmpSelectSql, CalciteConfig.MYSQL_LEX_CONFIG);
            SqlNode sqlNode = sqlParser.parseStmt();
            SqlBasicCall sqlBasicCall = buildAsSqlNode(tableAlias, sqlNode);
            queueInfo.offer(sqlBasicCall);

            System.out.println("-------build temporary query-----------");
            System.out.println(tmpSelectSql);
            System.out.println("---------------------------------------");
        }catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * 抽取上层需用使用到的字段
     * 由于where字段已经抽取到上一层了所以不用查询出来
     * @param parentSelectList
     * @param fromTableNameSet
     * @return
     */
    private Set<String> extractSelectFields(SqlNodeList parentSelectList,
                                            Set<String> fromTableNameSet,
                                            Map<String, String> tableRef){
        Set<String> extractFieldList = Sets.newHashSet();
        for(SqlNode selectNode : parentSelectList.getList()){
            extractSelectField(selectNode, extractFieldList, fromTableNameSet, tableRef);
        }

        return extractFieldList;
    }

    private Set<String> extractSelectFieldFromJoinCondition(Set<Tuple2<String, String>> joinFieldSet, Set<String> fromTableNameSet){
        Set<String> extractFieldList = Sets.newHashSet();
        for(Tuple2<String, String> field : joinFieldSet){
            if(fromTableNameSet.contains(field.f0)){
                extractFieldList.add(field.f0 + "." + field.f1);
            }
        }

        return extractFieldList;
    }

    /**
     * 从join的条件中获取字段信息
     * @param condition
     * @param joinFieldSet
     */
    private void extractJoinField(SqlNode condition, Set<Tuple2<String, String>> joinFieldSet){
        SqlKind joinKind = condition.getKind();
        if( joinKind == AND || joinKind == EQUALS ){
            extractJoinField(((SqlBasicCall)condition).operands[0], joinFieldSet);
            extractJoinField(((SqlBasicCall)condition).operands[1], joinFieldSet);
        }else{
            Preconditions.checkState(((SqlIdentifier)condition).names.size() == 2, "join condition must be format table.field");
            Tuple2<String, String> tuple2 = Tuple2.of(((SqlIdentifier)condition).names.get(0), ((SqlIdentifier)condition).names.get(1));
            joinFieldSet.add(tuple2);
        }
    }


    private void extractSelectField(SqlNode selectNode,
                                    Set<String> extractFieldSet,
                                    Set<String> fromTableNameSet,
                                    Map<String, String> tableRef){
        if (selectNode.getKind() == AS) {
            SqlNode leftNode = ((SqlBasicCall) selectNode).getOperands()[0];
            extractSelectField(leftNode, extractFieldSet, fromTableNameSet, tableRef);

        }else if(selectNode.getKind() == IDENTIFIER) {
            SqlIdentifier sqlIdentifier = (SqlIdentifier) selectNode;

            if(sqlIdentifier.names.size() == 1){
                return;
            }

            String tableName = sqlIdentifier.names.get(0);
            //TODO
            if(fromTableNameSet.contains(tableName)){
                extractFieldSet.add(sqlIdentifier.toString());
            } else if(fromTableNameSet.contains(tableRef.get(tableName))){
                //TODO extractFieldSet.add(sqlIdentifier.setName(0, tableRef.get(tableName)).toString());
            }

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

                extractSelectField(sqlNode, extractFieldSet, fromTableNameSet, tableRef);
            }

        }else if(selectNode.getKind() == CASE){
            System.out.println("selectNode");
            SqlCase sqlCase = (SqlCase) selectNode;
            SqlNodeList whenOperands = sqlCase.getWhenOperands();
            SqlNodeList thenOperands = sqlCase.getThenOperands();
            SqlNode elseNode = sqlCase.getElseOperand();

            for(int i=0; i<whenOperands.size(); i++){
                SqlNode oneOperand = whenOperands.get(i);
                extractSelectField(oneOperand, extractFieldSet, fromTableNameSet, tableRef);
            }

            for(int i=0; i<thenOperands.size(); i++){
                SqlNode oneOperand = thenOperands.get(i);
                extractSelectField(oneOperand, extractFieldSet, fromTableNameSet, tableRef);
            }

            extractSelectField(elseNode, extractFieldSet, fromTableNameSet, tableRef);
        }else {
            //do nothing
        }
    }


    private Tuple2<String, String> parseRightNode(SqlNode sqlNode, Set<String> sideTableSet, Queue<Object> queueInfo,
                                                  SqlNode parentWhere, SqlNodeList selectList) {
        Tuple2<String, String> tabName = new Tuple2<>("", "");
        if(sqlNode.getKind() == IDENTIFIER){
            tabName.f0 = sqlNode.toString();
        }else{
            AliasInfo aliasInfo = (AliasInfo)sideSQLParser.parseSql(sqlNode, sideTableSet, queueInfo, parentWhere, selectList);
            tabName.f0 = aliasInfo.getName();
            tabName.f1 = aliasInfo.getAlias();
        }
        return tabName;
    }

    private Tuple2<String, String> parseLeftNode(SqlNode sqlNode){
        Tuple2<String, String> tabName = new Tuple2<>("", "");
        if(sqlNode.getKind() == IDENTIFIER){
            tabName.f0 = sqlNode.toString();
            tabName.f1 = sqlNode.toString();
        }else if (sqlNode.getKind() == AS){
            SqlNode info = ((SqlBasicCall)sqlNode).getOperands()[0];
            SqlNode alias = ((SqlBasicCall) sqlNode).getOperands()[1];

            tabName.f0 = info.toString();
            tabName.f1 = alias.toString();
        }else {
            throw new RuntimeException("");
        }

        return tabName;
    }

    public String buildCondition(List<SqlBasicCall> conditionList){
        if(CollectionUtils.isEmpty(conditionList)){
            return "";
        }

        return " where " + StringUtils.join(conditionList, " AND ");
    }

    public String buildSelectNode(Set<String> extractSelectField, Set<String> joinFieldSet){
        if(CollectionUtils.isEmpty(extractSelectField)){
            throw new RuntimeException("no field is used");
        }

        Sets.SetView view = Sets.union(extractSelectField, joinFieldSet);

        return StringUtils.join(view, ",");
    }

    private boolean checkIsSideTable(String tableName, Set<String> sideTableList){
        if(sideTableList.contains(tableName)){
            return true;
        }
        return false;
    }

    private SqlBasicCall buildAsSqlNode(String internalTableName, SqlNode newSource) {
        SqlOperator operator = new SqlAsOperator();
        SqlParserPos sqlParserPos = new SqlParserPos(0, 0);
        SqlIdentifier sqlIdentifierAlias = new SqlIdentifier(internalTableName, null, sqlParserPos);
        SqlNode[] sqlNodes = new SqlNode[2];
        sqlNodes[0] = newSource;
        sqlNodes[1] = sqlIdentifierAlias;
        return new SqlBasicCall(operator, sqlNodes, sqlParserPos);
    }


    /**
     * 检查关联的where 条件中的判断是否可以下移到新构建的子查询
     * @param fromTableNameSet
     * @param parentWhere
     * @param extractCondition
     * @return
     */
    private boolean checkAndRemoveWhereCondition(Set<String> fromTableNameSet,
                                                 SqlBasicCall parentWhere,
                                                 List<SqlBasicCall> extractCondition){
        if(parentWhere == null){
            return false;
        }

        SqlKind kind = parentWhere.getKind();
        if(kind == AND){
            boolean removeLeft = checkAndRemoveWhereCondition(fromTableNameSet, (SqlBasicCall) parentWhere.getOperands()[0], extractCondition);
            boolean removeRight = checkAndRemoveWhereCondition(fromTableNameSet, (SqlBasicCall) parentWhere.getOperands()[1], extractCondition);
            //DO remove
            if(removeLeft){
                extractCondition.add(removeWhereConditionNode(parentWhere, 0));
            }

            if(removeRight){
                extractCondition.add(removeWhereConditionNode(parentWhere, 1));
            }

            return false;
        } else {
            Set<String> conditionRefTableNameSet = Sets.newHashSet();
            TableUtils.getConditionRefTable(parentWhere, conditionRefTableNameSet);

            if(fromTableNameSet.containsAll(conditionRefTableNameSet)){
                return true;
            }

            return false;
        }
    }

    /**
     * 抽取where 条件中指定的条件
     * @param parentWhere
     * @param index
     * @return
     */
    public SqlBasicCall removeWhereConditionNode(SqlBasicCall parentWhere, int index){
        SqlBasicCall oldCondition = (SqlBasicCall) parentWhere.getOperands()[index];
        parentWhere.setOperand(index, buildEmptyCondition());
        return oldCondition;
    }

    /**
     * 构建 1=1的 where 条件
     * @return
     */
    public SqlBasicCall buildEmptyCondition(){
        SqlBinaryOperator equalsOperators = SqlStdOperatorTable.EQUALS;
        SqlNode[] operands = new SqlNode[2];
        operands[0] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        operands[1] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);

        return new SqlBasicCall(equalsOperators, operands, SqlParserPos.ZERO);
    }

    /**
     * 替换join 条件中的表名称
     * @param node
     * @param tableMap 表名的关联关系
     */
    private SqlIdentifier checkAndReplaceJoinCondition(SqlNode node, Map<String, String> tableMap){

        SqlKind joinKind = node.getKind();
        if( joinKind == AND || joinKind == EQUALS ){
            SqlIdentifier leftNode = checkAndReplaceJoinCondition(((SqlBasicCall)node).operands[0], tableMap);
            SqlIdentifier rightNode = checkAndReplaceJoinCondition(((SqlBasicCall)node).operands[1], tableMap);

            if(leftNode != null){
                ((SqlBasicCall)node).setOperand(0, leftNode);
            }

            if(rightNode != null){
                ((SqlBasicCall)node).setOperand(1, leftNode);
            }

            return null;
        } else {
            //replace table
            Preconditions.checkState(((SqlIdentifier)node).names.size() == 2, "join condition must be format table.field");
            String tbName = ((SqlIdentifier) node).names.get(0);
            if(tableMap.containsKey(tbName)){
                tbName = tableMap.get(tbName);
                return ((SqlIdentifier) node).setName(0, tbName);
            }

            return null;
        }
    }



}
