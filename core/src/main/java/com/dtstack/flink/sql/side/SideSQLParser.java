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
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static org.apache.calcite.sql.SqlKind.*;

/**
 * Parsing sql, obtain execution information dimension table
 * Date: 2018/7/24
 * Company: www.dtstack.com
 * @author xuchao
 */

public class SideSQLParser {
    private static final Logger LOG = LoggerFactory.getLogger(SideSQLParser.class);

    private Map<String, Table> localTableCache = Maps.newHashMap();

    //用来构建临时的中间查询
    private static final String SELECT_TEMP_SQL = "select %s from %s %s";

    public Queue<Object> getExeQueue(String exeSql, Set<String> sideTableSet) throws SqlParseException {
        System.out.println("----------exec original Sql----------");
        System.out.println(exeSql);
        LOG.info("----------exec original Sql----------");
        LOG.info(exeSql);

        Queue<Object> queueInfo = Queues.newLinkedBlockingQueue();
        SqlParser sqlParser = SqlParser.create(exeSql, CalciteConfig.MYSQL_LEX_CONFIG);
        SqlNode sqlNode = sqlParser.parseStmt();

        parseSql(sqlNode, sideTableSet, queueInfo, null, null);
        queueInfo.offer(sqlNode);
        return queueInfo;
    }

    private void checkAndReplaceMultiJoin(SqlNode sqlNode, Set<String> sideTableSet) {
        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind) {
            case WITH: {
                SqlWith sqlWith = (SqlWith) sqlNode;
                SqlNodeList sqlNodeList = sqlWith.withList;
                for (SqlNode withAsTable : sqlNodeList) {
                    SqlWithItem sqlWithItem = (SqlWithItem) withAsTable;
                    checkAndReplaceMultiJoin(sqlWithItem.query, sideTableSet);
                }
                checkAndReplaceMultiJoin(sqlWith.body, sideTableSet);
                break;
            }
            case INSERT:
                SqlNode sqlSource = ((SqlInsert) sqlNode).getSource();
                checkAndReplaceMultiJoin(sqlSource, sideTableSet);
                break;
            case SELECT:
                SqlNode sqlFrom = ((SqlSelect) sqlNode).getFrom();
                if (sqlFrom.getKind() != IDENTIFIER) {
                    checkAndReplaceMultiJoin(sqlFrom, sideTableSet);
                }
                break;
            case JOIN:
                convertSideJoinToNewQuery((SqlJoin) sqlNode, sideTableSet);
                break;
            case AS:
                SqlNode info = ((SqlBasicCall) sqlNode).getOperands()[0];
                if (info.getKind() != IDENTIFIER) {
                    checkAndReplaceMultiJoin(info, sideTableSet);
                }
                break;
            case UNION:
                SqlNode unionLeft = ((SqlBasicCall) sqlNode).getOperands()[0];
                SqlNode unionRight = ((SqlBasicCall) sqlNode).getOperands()[1];
                checkAndReplaceMultiJoin(unionLeft, sideTableSet);
                checkAndReplaceMultiJoin(unionRight, sideTableSet);
                break;
        }
    }


    private Object parseSql(SqlNode sqlNode, Set<String> sideTableSet, Queue<Object> queueInfo, SqlNode parentWhere, SqlNodeList parentSelectList){
        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind){
            case WITH: {
                SqlWith sqlWith = (SqlWith) sqlNode;
                SqlNodeList sqlNodeList = sqlWith.withList;
                for (SqlNode withAsTable : sqlNodeList) {
                    SqlWithItem sqlWithItem = (SqlWithItem) withAsTable;
                    parseSql(sqlWithItem.query, sideTableSet, queueInfo, parentWhere, parentSelectList);
                    queueInfo.add(sqlWithItem);
                }
                parseSql(sqlWith.body, sideTableSet, queueInfo, parentWhere, parentSelectList);
                break;
            }
            case INSERT:
                SqlNode sqlSource = ((SqlInsert)sqlNode).getSource();
                return parseSql(sqlSource, sideTableSet, queueInfo, parentWhere, parentSelectList);
            case SELECT:
                SqlNode sqlFrom = ((SqlSelect)sqlNode).getFrom();
                SqlNode sqlWhere = ((SqlSelect)sqlNode).getWhere();
                SqlNodeList selectList = ((SqlSelect)sqlNode).getSelectList();

                if(sqlFrom.getKind() != IDENTIFIER){
                    Object result = parseSql(sqlFrom, sideTableSet, queueInfo, sqlWhere, selectList);
                    if(result instanceof JoinInfo){
                        return TableUtils.dealSelectResultWithJoinInfo((JoinInfo) result, (SqlSelect) sqlNode, queueInfo);
                    }else if(result instanceof AliasInfo){
                        String tableName = ((AliasInfo) result).getName();
                        if(sideTableSet.contains(tableName)){
                            throw new RuntimeException("side-table must be used in join operator");
                        }
                    }
                }else{
                    String tableName = ((SqlIdentifier)sqlFrom).getSimple();
                    if(sideTableSet.contains(tableName)){
                        throw new RuntimeException("side-table must be used in join operator");
                    }
                }
                break;
            case JOIN:
                Set<Tuple2<String, String>> joinFieldSet = Sets.newHashSet();
                return dealJoinNode((SqlJoin) sqlNode, sideTableSet, queueInfo, parentWhere, parentSelectList, joinFieldSet);
            case AS:
                SqlNode info = ((SqlBasicCall)sqlNode).getOperands()[0];
                SqlNode alias = ((SqlBasicCall) sqlNode).getOperands()[1];
                String infoStr = "";

                if(info.getKind() == IDENTIFIER){
                    infoStr = info.toString();
                } else {
                    infoStr = parseSql(info, sideTableSet, queueInfo, parentWhere, parentSelectList).toString();
                }

                AliasInfo aliasInfo = new AliasInfo();
                aliasInfo.setName(infoStr);
                aliasInfo.setAlias(alias.toString());

                return aliasInfo;

            case UNION:
                SqlNode unionLeft = ((SqlBasicCall)sqlNode).getOperands()[0];
                SqlNode unionRight = ((SqlBasicCall)sqlNode).getOperands()[1];

                parseSql(unionLeft, sideTableSet, queueInfo, parentWhere, parentSelectList);
                parseSql(unionRight, sideTableSet, queueInfo, parentWhere, parentSelectList);
                break;
            case ORDER_BY:
                SqlOrderBy sqlOrderBy  = (SqlOrderBy) sqlNode;
                parseSql(sqlOrderBy.query, sideTableSet, queueInfo, parentWhere, parentSelectList);
        }
        return "";
    }

    private AliasInfo getSqlNodeAliasInfo(SqlNode sqlNode) {
        SqlNode info = ((SqlBasicCall) sqlNode).getOperands()[0];
        SqlNode alias = ((SqlBasicCall) sqlNode).getOperands()[1];
        String infoStr = info.getKind() == IDENTIFIER ? info.toString() : null;

        AliasInfo aliasInfo = new AliasInfo();
        aliasInfo.setName(infoStr);
        aliasInfo.setAlias(alias.toString());
        return aliasInfo;
    }

    /**
     * 将和维表关联的join 替换为一个新的查询
     * @param sqlNode
     * @param sideTableSet
     */
    private void convertSideJoinToNewQuery(SqlJoin sqlNode, Set<String> sideTableSet) {
        checkAndReplaceMultiJoin(sqlNode.getLeft(), sideTableSet);
        checkAndReplaceMultiJoin(sqlNode.getRight(), sideTableSet);
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
     * 解析 join 操作
     * @param joinNode
     * @param sideTableSet
     * @param queueInfo
     * @param parentWhere
     * @param parentSelectList
     * @return
     */
    private JoinInfo dealJoinNode(SqlJoin joinNode, Set<String> sideTableSet, Queue<Object> queueInfo,
                                  SqlNode parentWhere, SqlNodeList parentSelectList, Set<Tuple2<String, String>> joinFieldSet) {
        SqlNode leftNode = joinNode.getLeft();
        SqlNode rightNode = joinNode.getRight();
        JoinType joinType = joinNode.getJoinType();

        String leftTbName = "";
        String leftTbAlias = "";
        String rightTableName = "";
        String rightTableAlias = "";
        boolean leftTbisTmp = false;

        //如果是连续join 判断是否已经处理过添加到执行队列
        Boolean alreadyOffer = false;
        extractJoinField(joinNode.getCondition(), joinFieldSet);

        if(leftNode.getKind() == IDENTIFIER){
            leftTbName = leftNode.toString();
        } else if (leftNode.getKind() == JOIN) {
            //处理连续join
            Tuple2<Boolean, SqlBasicCall> nestJoinResult = dealNestJoin((SqlJoin) leftNode, sideTableSet,
                    queueInfo, parentWhere, parentSelectList, joinFieldSet);
            alreadyOffer = nestJoinResult.f0;
            leftTbName = nestJoinResult.f1.getOperands()[0].toString();
            leftTbAlias = nestJoinResult.f1.getOperands()[1].toString();
            leftTbisTmp = true;
        } else if (leftNode.getKind() == AS) {
            AliasInfo aliasInfo = (AliasInfo) parseSql(leftNode, sideTableSet, queueInfo, parentWhere, parentSelectList);
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

        tableInfo.setLeftIsTmpTable(leftTbisTmp);
        tableInfo.setLeftIsSideTable(leftIsSide);
        tableInfo.setRightIsSideTable(rightIsSide);
        tableInfo.setLeftNode(leftNode);
        tableInfo.setRightNode(rightNode);
        tableInfo.setJoinType(joinType);
        tableInfo.setCondition(joinNode.getCondition());

        if(!rightIsSide || alreadyOffer){
            return tableInfo;
        }

        if(tableInfo.getLeftNode().getKind() != AS){
            extractTemporaryQuery(tableInfo.getLeftNode(), tableInfo.getLeftTableAlias(), (SqlBasicCall) parentWhere,
                    parentSelectList, queueInfo, joinFieldSet);
        }else {
            SqlKind asNodeFirstKind = ((SqlBasicCall)tableInfo.getLeftNode()).operands[0].getKind();
            if(asNodeFirstKind == SELECT){
                queueInfo.offer(tableInfo.getLeftNode());
                tableInfo.setLeftNode(((SqlBasicCall)tableInfo.getLeftNode()).operands[1]);
            }
        }
        return tableInfo;
    }


    //构建新的查询
    private Tuple2<Boolean, SqlBasicCall> dealNestJoin(SqlJoin joinNode, Set<String> sideTableSet,
                                                       Queue<Object> queueInfo, SqlNode parentWhere,
                                                       SqlNodeList selectList, Set<Tuple2<String, String>> joinFieldSet){
        SqlNode rightNode = joinNode.getRight();
        Tuple2<String, String> rightTableNameAndAlias = parseRightNode(rightNode, sideTableSet, queueInfo, parentWhere, selectList);
        JoinInfo joinInfo = dealJoinNode(joinNode, sideTableSet, queueInfo, parentWhere, selectList, joinFieldSet);

        String rightTableName = rightTableNameAndAlias.f0;
        boolean rightIsSide = checkIsSideTable(rightTableName, sideTableSet);
        boolean alreadyOffer = false;

        if(!rightIsSide){
            //右表不是维表的情况
        }else{
            //右边表是维表需要重新构建左表的临时查询
            queueInfo.offer(joinInfo);
            alreadyOffer = true;
        }

        return Tuple2.of(alreadyOffer, TableUtils.buildAsNodeByJoinInfo(joinInfo, null, null));
    }

    public boolean checkAndRemoveCondition(Set<String> fromTableNameSet, SqlBasicCall parentWhere, List<SqlBasicCall> extractCondition){

        if(parentWhere == null){
            return false;
        }

        SqlKind kind = parentWhere.getKind();
        if(kind == AND){
            boolean removeLeft = checkAndRemoveCondition(fromTableNameSet, (SqlBasicCall) parentWhere.getOperands()[0], extractCondition);
            boolean removeRight = checkAndRemoveCondition(fromTableNameSet, (SqlBasicCall) parentWhere.getOperands()[1], extractCondition);
            //DO remove
            if(removeLeft){
                extractCondition.add(removeWhereConditionNode(parentWhere, 0));
            }

            if(removeRight){
                extractCondition.add(removeWhereConditionNode(parentWhere, 1));
            }

            return false;
        }else{
            Set<String> conditionRefTableNameSet = Sets.newHashSet();
            getConditionRefTable(parentWhere, conditionRefTableNameSet);

            if(fromTableNameSet.containsAll(conditionRefTableNameSet)){
                return true;
            }

            return false;
        }
    }

    private void extractTemporaryQuery(SqlNode node, String tableAlias, SqlBasicCall parentWhere,
                                       SqlNodeList parentSelectList, Queue<Object> queueInfo,
                                       Set<Tuple2<String, String>> joinFieldSet){
        try{
            //父一级的where 条件中如果只和临时查询相关的条件都截取进来
            Set<String> fromTableNameSet = Sets.newHashSet();
            List<SqlBasicCall> extractCondition = Lists.newArrayList();

            getFromTableInfo(node, fromTableNameSet);
            checkAndRemoveCondition(fromTableNameSet, parentWhere, extractCondition);

            Set<String> extractSelectField = extractSelectFields(parentSelectList, fromTableNameSet);
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
    private Set<String> extractSelectFields(SqlNodeList parentSelectList, Set<String> fromTableNameSet){
        Set<String> extractFieldList = Sets.newHashSet();
        for(SqlNode selectNode : parentSelectList.getList()){
            extractSelectField(selectNode, extractFieldList, fromTableNameSet);
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

    private void extractSelectField(SqlNode selectNode, Set<String> extractFieldSet, Set<String> fromTableNameSet){
        if (selectNode.getKind() == AS) {
            SqlNode leftNode = ((SqlBasicCall) selectNode).getOperands()[0];
            extractSelectField(leftNode, extractFieldSet, fromTableNameSet);

        }else if(selectNode.getKind() == IDENTIFIER) {
            SqlIdentifier sqlIdentifier = (SqlIdentifier) selectNode;

            if(sqlIdentifier.names.size() == 1){
               return;
            }

            String tableName = sqlIdentifier.names.get(0);
            if(fromTableNameSet.contains(tableName)){
                extractFieldSet.add(sqlIdentifier.toString());
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

                extractSelectField(sqlNode, extractFieldSet, fromTableNameSet);
            }

        }else if(selectNode.getKind() == CASE){
            System.out.println("selectNode");
            SqlCase sqlCase = (SqlCase) selectNode;
            SqlNodeList whenOperands = sqlCase.getWhenOperands();
            SqlNodeList thenOperands = sqlCase.getThenOperands();
            SqlNode elseNode = sqlCase.getElseOperand();

            for(int i=0; i<whenOperands.size(); i++){
                SqlNode oneOperand = whenOperands.get(i);
                extractSelectField(oneOperand, extractFieldSet, fromTableNameSet);
            }

            for(int i=0; i<thenOperands.size(); i++){
                SqlNode oneOperand = thenOperands.get(i);
                extractSelectField(oneOperand, extractFieldSet, fromTableNameSet);
            }

            extractSelectField(elseNode, extractFieldSet, fromTableNameSet);
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
            AliasInfo aliasInfo = (AliasInfo)parseSql(sqlNode, sideTableSet, queueInfo, parentWhere, selectList);
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

    public SqlBasicCall removeWhereConditionNode(SqlBasicCall parentWhere, int index){
        //构造1=1 条件
        SqlBasicCall oldCondition = (SqlBasicCall) parentWhere.getOperands()[index];
        parentWhere.setOperand(index, buildDefaultCondition());
        return oldCondition;
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

    public SqlBasicCall buildDefaultCondition(){
        SqlBinaryOperator equalsOperators = SqlStdOperatorTable.EQUALS;
        SqlNode[] operands = new SqlNode[2];
        operands[0] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        operands[1] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);

        return new SqlBasicCall(equalsOperators, operands, SqlParserPos.ZERO);
    }


    private boolean checkIsSideTable(String tableName, Set<String> sideTableList){
        if(sideTableList.contains(tableName)){
            return true;
        }
        return false;
    }

    public void setLocalTableCache(Map<String, Table> localTableCache) {
        this.localTableCache = localTableCache;
    }

    //TODO 之后抽取
    private void getConditionRefTable(SqlNode selectNode, Set<String> tableNameSet) {
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


    public void getFromTableInfo(SqlNode fromTable, Set<String> tableNameSet){
        System.out.println(fromTable);
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
            default:
                throw new RuntimeException("not support sqlKind:" + sqlKind);
        }
    }
}
