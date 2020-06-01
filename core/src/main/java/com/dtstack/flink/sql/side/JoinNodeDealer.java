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

import com.dtstack.flink.sql.parser.FlinkPlanner;
import com.dtstack.flink.sql.util.ParseUtils;
import com.dtstack.flink.sql.util.TableUtils;
import com.esotericsoftware.minlog.Log;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;
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
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.calcite.FlinkPlannerImpl;

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
     * @param queueInfo sql执行队列
     * @param parentWhere join 关联的最上层的where 节点
     * @param parentSelectList join 关联的最上层的select 节点
     * @param joinFieldSet
     * @param tableRef 存储构建临时表查询后源表和新表之间的关联关系
     * @return
     */
    public JoinInfo dealJoinNode(SqlJoin joinNode,
                                 Set<String> sideTableSet,
                                 Queue<Object> queueInfo,
                                 SqlNode parentWhere,
                                 SqlNodeList parentSelectList,
                                 SqlNodeList parentGroupByList,
                                 Set<Tuple2<String, String>> joinFieldSet,
                                 Map<String, String> tableRef,
                                 Map<String, String> fieldRef,
                                 String scope) {

        SqlNode leftNode = joinNode.getLeft();
        SqlNode rightNode = joinNode.getRight();
        JoinType joinType = joinNode.getJoinType();

        String leftTbName = "";
        String leftTbAlias = "";
        String rightTableName = "";
        String rightTableAlias = "";

        //抽取join中的的条件
        extractJoinField(joinNode.getCondition(), joinFieldSet);

        if (leftNode.getKind() == JOIN) {
            //处理连续join
            dealNestJoin(joinNode, sideTableSet, queueInfo, parentWhere, parentSelectList,
                    parentGroupByList, joinFieldSet, tableRef, fieldRef, scope);
            leftNode = joinNode.getLeft();
        }

        if (leftNode.getKind() == AS) {
            AliasInfo aliasInfo = (AliasInfo) sideSQLParser.parseSql(leftNode, sideTableSet, queueInfo,
                    parentWhere, parentSelectList, parentGroupByList, scope);
            leftTbName = aliasInfo.getName();
            leftTbAlias = aliasInfo.getAlias();
        } else if(leftNode.getKind() == IDENTIFIER){
            leftTbName = leftNode.toString();
            leftTbAlias = leftTbName;
        }

        boolean leftIsSide = checkIsSideTable(leftTbName, sideTableSet);
        Preconditions.checkState(!leftIsSide, "side-table must be at the right of join operator");

        Tuple2<String, String> rightTableNameAndAlias = parseRightNode(rightNode, sideTableSet, queueInfo,
                parentWhere, parentSelectList, parentGroupByList, scope);
        rightTableName = rightTableNameAndAlias.f0;
        rightTableAlias = rightTableNameAndAlias.f1;

        boolean rightIsSide = checkIsSideTable(rightTableName, sideTableSet);
        if(rightIsSide && joinType == JoinType.RIGHT){
            throw new RuntimeException("side join not support join type of right[current support inner join and left join]");
        }

        JoinInfo tableInfo = new JoinInfo();
        tableInfo.setLeftTableName(leftTbName);
        tableInfo.setRightTableName(rightTableName);

        leftTbAlias = StringUtils.isEmpty(leftTbAlias) ? leftTbName : leftTbAlias;
        rightTableAlias = StringUtils.isEmpty(rightTableAlias) ? rightTableName : rightTableAlias;

        tableInfo.setLeftTableAlias(leftTbAlias);
        tableInfo.setRightTableAlias(rightTableAlias);
        tableInfo.setRightIsSideTable(rightIsSide);
        tableInfo.setLeftNode(leftNode);
        tableInfo.setRightNode(rightNode);
        tableInfo.setJoinType(joinType);
        tableInfo.setCondition(joinNode.getCondition());
        tableInfo.setScope(scope);

        TableUtils.replaceJoinFieldRefTableName(joinNode.getCondition(), fieldRef);

        //extract 需要查询的字段信息
        if(rightIsSide){
            extractJoinNeedSelectField(leftNode, rightNode, parentWhere, parentSelectList, parentGroupByList, tableRef, joinFieldSet, fieldRef, tableInfo);
        }

        if(tableInfo.getLeftNode().getKind() != AS){
            return tableInfo;
        }

        SqlKind asNodeFirstKind = ((SqlBasicCall)tableInfo.getLeftNode()).operands[0].getKind();
        if(asNodeFirstKind == SELECT){
            queueInfo.offer(tableInfo.getLeftNode());
            tableInfo.setLeftNode(((SqlBasicCall)tableInfo.getLeftNode()).operands[1]);
        }

        return tableInfo;
    }

    /**
     * 获取join 之后需要查询的字段信息
     */
    public void extractJoinNeedSelectField(SqlNode leftNode,
                                           SqlNode rightNode,
                                           SqlNode parentWhere,
                                           SqlNodeList parentSelectList,
                                           SqlNodeList parentGroupByList,
                                           Map<String, String> tableRef,
                                           Set<Tuple2<String, String>> joinFieldSet,
                                           Map<String, String> fieldRef,
                                           JoinInfo tableInfo){

        Set<String> extractSelectField = extractField(leftNode, parentWhere, parentSelectList, parentGroupByList, tableRef, joinFieldSet);
        Set<String> rightExtractSelectField = extractField(rightNode, parentWhere, parentSelectList, parentGroupByList, tableRef, joinFieldSet);

        //重命名right 中和 left 重名的
        Map<String, String> leftTbSelectField = Maps.newHashMap();
        Map<String, String> rightTbSelectField = Maps.newHashMap();
        String newTableName = tableInfo.getNewTableAlias();

        for(String tmpField : extractSelectField){
            String[] tmpFieldSplit = StringUtils.split(tmpField, '.');
            leftTbSelectField.put(tmpFieldSplit[1], tmpFieldSplit[1]);
            fieldRef.put(tmpField, TableUtils.buildTableField(newTableName, tmpFieldSplit[1]));
        }

        for(String tmpField : rightExtractSelectField){
            String[] tmpFieldSplit = StringUtils.split(tmpField, '.');
            String originalFieldName = tmpFieldSplit[1];
            String targetFieldName = originalFieldName;
            if(leftTbSelectField.containsKey(originalFieldName)){
                targetFieldName = ParseUtils.dealDuplicateFieldName(leftTbSelectField, originalFieldName);
            }

            rightTbSelectField.put(originalFieldName, targetFieldName);
            fieldRef.put(tmpField, TableUtils.buildTableField(newTableName, targetFieldName));
        }

        tableInfo.setLeftSelectFieldInfo(leftTbSelectField);
        tableInfo.setRightSelectFieldInfo(rightTbSelectField);
    }

    /**
     * 指定的节点关联到的 select 中的字段和 where中的字段
     * @param sqlNode
     * @param parentWhere
     * @param parentSelectList
     * @param parentGroupByList
     * @param tableRef
     * @param joinFieldSet
     * @return
     */
    public Set<String> extractField(SqlNode sqlNode,
                                    SqlNode parentWhere,
                                    SqlNodeList parentSelectList,
                                    SqlNodeList parentGroupByList,
                                    Map<String, String> tableRef,
                                    Set<Tuple2<String, String>> joinFieldSet){
        Set<String> fromTableNameSet = Sets.newHashSet();
        TableUtils.getFromTableInfo(sqlNode, fromTableNameSet);
        Set<String> extractCondition = Sets.newHashSet();

        extractWhereCondition(fromTableNameSet, (SqlBasicCall) parentWhere, extractCondition);
        Set<String> extractSelectField = extractSelectFields(parentSelectList, fromTableNameSet, tableRef);
        Set<String> fieldFromJoinCondition = extractSelectFieldFromJoinCondition(joinFieldSet, fromTableNameSet, tableRef);

        Set<String> extractGroupByField = extractFieldFromGroupByList(parentGroupByList, fromTableNameSet, tableRef);
        extractSelectField.addAll(extractCondition);
        extractSelectField.addAll(fieldFromJoinCondition);
        extractSelectField.addAll(extractGroupByField);

        return extractSelectField;
    }


    /**
     * 处理多层join
     * 判断左节点是否需要创建临时查询
     * （1）右节点是维表
     * （2）左节点不是 as 节点
     */
    private JoinInfo dealNestJoin(SqlJoin joinNode,
                                  Set<String> sideTableSet,
                                  Queue<Object> queueInfo,
                                  SqlNode parentWhere,
                                  SqlNodeList parentSelectList,
                                  SqlNodeList parentGroupByList,
                                  Set<Tuple2<String, String>> joinFieldSet,
                                  Map<String, String> tableRef,
                                  Map<String, String> fieldRef,
                                  String scope){

        SqlJoin leftJoinNode = (SqlJoin) joinNode.getLeft();
        SqlNode parentRightJoinNode = joinNode.getRight();
        SqlNode rightNode = leftJoinNode.getRight();

        Tuple2<String, String> rightTableNameAndAlias = parseRightNode(rightNode, sideTableSet, queueInfo,
                parentWhere, parentSelectList, parentGroupByList, scope);
        Tuple2<String, String> parentRightJoinInfo = parseRightNode(parentRightJoinNode, sideTableSet,
                queueInfo, parentWhere, parentSelectList, parentGroupByList, scope);
        boolean parentRightIsSide = checkIsSideTable(parentRightJoinInfo.f0, sideTableSet);

        JoinInfo joinInfo = dealJoinNode(leftJoinNode, sideTableSet, queueInfo, parentWhere, parentSelectList,
                parentGroupByList, joinFieldSet, tableRef, fieldRef, scope);

        String rightTableName = rightTableNameAndAlias.f0;
        boolean rightIsSide = checkIsSideTable(rightTableName, sideTableSet);
        SqlBasicCall buildAs = TableUtils.buildAsNodeByJoinInfo(joinInfo, null, null);

        if(rightIsSide){
            addSideInfoToExeQueue(queueInfo, joinInfo, joinNode, parentSelectList, parentGroupByList, parentWhere, tableRef);
        }

        SqlNode newLeftNode = joinNode.getLeft();

        if(newLeftNode.getKind() != AS && parentRightIsSide){

            String leftTbAlias = buildAs.getOperands()[1].toString();
            extractTemporaryQuery(newLeftNode, leftTbAlias, (SqlBasicCall) parentWhere,
                    parentSelectList, queueInfo, joinFieldSet, tableRef, fieldRef);

            //替换leftNode 为新的查询
            joinNode.setLeft(buildAs);
            replaceSelectAndWhereField(buildAs, leftJoinNode, tableRef, parentSelectList, parentGroupByList, parentWhere);
        }

        return joinInfo;
    }

    /**
     * 右边表是维表需要重新构建左表的临时查询
     * 并将joinInfo 添加到执行队列里面
     * @param queueInfo
     * @param joinInfo
     * @param joinNode
     * @param parentSelectList
     * @param parentGroupByList
     * @param parentWhere
     * @param tableRef
     */
    public void addSideInfoToExeQueue(Queue<Object> queueInfo,
                                      JoinInfo joinInfo,
                                      SqlJoin joinNode,
                                      SqlNodeList parentSelectList,
                                      SqlNodeList parentGroupByList,
                                      SqlNode parentWhere,
                                      Map<String, String> tableRef){
        //只处理维表
        if(!joinInfo.isRightIsSideTable()){
            return;
        }

        SqlBasicCall buildAs = TableUtils.buildAsNodeByJoinInfo(joinInfo, null, null);
        SqlNode leftJoinNode = joinNode.getLeft();
        queueInfo.offer(joinInfo);
        //替换左表为新的表名称
        joinNode.setLeft(buildAs);

        replaceSelectAndWhereField(buildAs, leftJoinNode, tableRef, parentSelectList, parentGroupByList, parentWhere);
    }

    /**
     * 替换指定的查询和条件节点中的字段为新的字段
     * @param buildAs
     * @param leftJoinNode
     * @param tableRef
     * @param parentSelectList
     * @param parentGroupByList
     * @param parentWhere
     */
    public void replaceSelectAndWhereField(SqlBasicCall buildAs,
                   SqlNode leftJoinNode,
                   Map<String, String> tableRef,
                   SqlNodeList parentSelectList,
                   SqlNodeList parentGroupByList,
                   SqlNode parentWhere){

        String newLeftTableName = buildAs.getOperands()[1].toString();
        Set<String> fromTableNameSet = Sets.newHashSet();
        TableUtils.getFromTableInfo(leftJoinNode, fromTableNameSet);

        for(String tbTmp : fromTableNameSet){
            tableRef.put(tbTmp, newLeftTableName);
        }

        //替换select field 中的对应字段
        HashBiMap<String, String> fieldReplaceRef = HashBiMap.create();
        for(SqlNode sqlNode : parentSelectList.getList()){
            for(String tbTmp : fromTableNameSet) {
                TableUtils.replaceSelectFieldTable(sqlNode, tbTmp, newLeftTableName, fieldReplaceRef);
            }
        }

        //TODO 应该根据上面的查询字段的关联关系来替换
        //替换where 中的条件相关
        for(String tbTmp : fromTableNameSet){
            TableUtils.replaceWhereCondition(parentWhere, tbTmp, newLeftTableName, fieldReplaceRef);
        }

        if(parentGroupByList != null){
            for(SqlNode sqlNode : parentGroupByList.getList()){
                for(String tbTmp : fromTableNameSet) {
                    TableUtils.replaceSelectFieldTable(sqlNode, tbTmp, newLeftTableName, fieldReplaceRef);
                }
            }
        }

    }

    /**
     * 抽取出中间查询表
     * @param node
     * @param tableAlias
     * @param parentWhere
     * @param parentSelectList
     * @param queueInfo
     * @param joinFieldSet
     * @param tableRef
     * @return 源自段和新生成字段之间的映射关系
     */
    private void extractTemporaryQuery(SqlNode node, String tableAlias,
                                       SqlBasicCall parentWhere,
                                       SqlNodeList parentSelectList,
                                       Queue<Object> queueInfo,
                                       Set<Tuple2<String, String>> joinFieldSet,
                                       Map<String, String> tableRef,
                                       Map<String, String> fieldRef){
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
            Set<String> fieldFromJoinCondition = extractSelectFieldFromJoinCondition(joinFieldSet, fromTableNameSet, tableRef);
            Set<String> newFields = buildSelectNode(extractSelectField, fieldFromJoinCondition);
            String extractSelectFieldStr = StringUtils.join(newFields, ',');

            Map<String, String> oldRefNewField = buildTmpTableFieldRefOriField(newFields, tableAlias);
            fieldRef.putAll(oldRefNewField);

            String extractConditionStr = buildCondition(extractCondition);

            String tmpSelectSql = String.format(SELECT_TEMP_SQL,
                    extractSelectFieldStr,
                    node.toString(),
                    extractConditionStr);

            FlinkPlannerImpl flinkPlanner = FlinkPlanner.getFlinkPlanner();
            SqlNode sqlNode = flinkPlanner.parse(tmpSelectSql);

            SqlBasicCall sqlBasicCall = buildAsSqlNode(tableAlias, sqlNode);
            queueInfo.offer(sqlBasicCall);

            //替换select中的表结构
            HashBiMap<String, String> fieldReplaceRef = HashBiMap.create();
            for(SqlNode tmpSelect : parentSelectList.getList()){
                for(String tbTmp : fromTableNameSet) {
                    TableUtils.replaceSelectFieldTable(tmpSelect, tbTmp, tableAlias, fieldReplaceRef);
                }
            }

            //替换where 中的条件相关
            for(String tbTmp : fromTableNameSet){
                TableUtils.replaceWhereCondition(parentWhere, tbTmp, tableAlias, fieldReplaceRef);
            }

            for(String tbTmp : fromTableNameSet){
                tableRef.put(tbTmp, tableAlias);
            }

            Log.info("-------build temporary query-----------\n{}", tmpSelectSql);
            Log.info("---------------------------------------");

        }catch (Exception e){
            Log.error("", e);
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

    private Set<String> extractSelectFieldFromJoinCondition(Set<Tuple2<String, String>> joinFieldSet,
                                                            Set<String> fromTableNameSet,
                                                            Map<String, String> tableRef){
        Set<String> extractFieldList = Sets.newHashSet();
        for(Tuple2<String, String> field : joinFieldSet){
            if(fromTableNameSet.contains(field.f0)){
                extractFieldList.add(field.f0 + "." + field.f1);
            }

            if(tableRef.containsKey(field.f0)){
                if(fromTableNameSet.contains(tableRef.get(field.f0))){
                    extractFieldList.add(tableRef.get(field.f0) + "." + field.f1);
                }
            }
        }

        return extractFieldList;
    }

    private Set<String> extractFieldFromGroupByList(SqlNodeList parentGroupByList,
                                                    Set<String> fromTableNameSet,
                                                    Map<String, String> tableRef){

        if(parentGroupByList == null){
            return Sets.newHashSet();
        }

        Set<String> extractFieldList = Sets.newHashSet();
        for(SqlNode selectNode : parentGroupByList.getList()){
            extractSelectField(selectNode, extractFieldList, fromTableNameSet, tableRef);
        }

        return extractFieldList;
    }

    /**
     * 从join的条件中获取字段信息
     * @param condition
     * @param joinFieldSet
     */
    private void extractJoinField(SqlNode condition, Set<Tuple2<String, String>> joinFieldSet){
        if (null == condition || condition.getKind() == LITERAL) {
            return;
        }

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

                extractSelectField(sqlNode, extractFieldSet, fromTableNameSet, tableRef);
            }

        }else if(selectNode.getKind() == CASE){
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
                                                  SqlNode parentWhere, SqlNodeList selectList, SqlNodeList parentGroupByList,
                                                  String scope) {
        Tuple2<String, String> tabName = new Tuple2<>("", "");
        if(sqlNode.getKind() == IDENTIFIER){
            tabName.f0 = sqlNode.toString();
        }else{
            AliasInfo aliasInfo = (AliasInfo)sideSQLParser.parseSql(sqlNode, sideTableSet, queueInfo, parentWhere, selectList, parentGroupByList, scope);
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

    /**
     * 构建抽取表的查询字段信息
     * 包括去除重复字段，名称相同的取别名
     * @param extractSelectField
     * @param joinFieldSet
     * @return
     */
    public Set<String> buildSelectNode(Set<String> extractSelectField, Set<String> joinFieldSet){
        if(CollectionUtils.isEmpty(extractSelectField)){
            throw new RuntimeException("no field is used");
        }

        Sets.SetView<String> view = Sets.union(extractSelectField, joinFieldSet);
        Set<String> newFieldSet = Sets.newHashSet();
        //为相同的列取别名
        HashBiMap<String, String> refFieldMap = HashBiMap.create();
        for(String field : view){
            String[] fieldInfo = StringUtils.split(field, '.');
            String aliasName = fieldInfo[1];
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(field);
            if(refFieldMap.inverse().get(aliasName) != null){
                aliasName = ParseUtils.dealDuplicateFieldName(refFieldMap, aliasName);
                stringBuilder.append(" as ")
                        .append(aliasName);
            }

            refFieldMap.put(field, aliasName);

            newFieldSet.add(stringBuilder.toString());
        }

        return newFieldSet;
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
     * 获取where中和指定表有关联的字段
     * @param fromTableNameSet
     * @param parentWhere
     * @param extractCondition
     */
    private void extractWhereCondition(Set<String> fromTableNameSet, SqlBasicCall parentWhere, Set<String> extractCondition){

        if(parentWhere == null){
            return;
        }

        SqlKind kind = parentWhere.getKind();
        if(kind == AND){
            extractWhereCondition(fromTableNameSet, (SqlBasicCall) parentWhere.getOperands()[0], extractCondition);
            extractWhereCondition(fromTableNameSet, (SqlBasicCall) parentWhere.getOperands()[1], extractCondition);
        } else {

            Set<String> fieldInfos = Sets.newHashSet();
            TableUtils.getConditionRefTable(parentWhere, fieldInfos);
            fieldInfos.forEach(fieldInfo -> {
                String[] splitInfo = StringUtils.split(fieldInfo, ".");
                if(splitInfo.length == 2 && fromTableNameSet.contains(splitInfo[0])){
                    extractCondition.add(fieldInfo);
                }
            });

        }


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
            //条件表达式，如果该条件关联的表都是指定的表则移除
            Set<String> fieldInfos = Sets.newHashSet();
            TableUtils.getConditionRefTable(parentWhere, fieldInfos);
            Set<String> conditionRefTableNameSet = Sets.newHashSet();

            fieldInfos.forEach(fieldInfo -> {
                String[] splitInfo = StringUtils.split(fieldInfo, ".");
                if(splitInfo.length == 2){
                    conditionRefTableNameSet.add(splitInfo[0]);
                }
            });


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

    /**
     * 解析出临时中间表的属性列和源表之间的关系
     * @param fieldSet
     * @param newTableAliasName
     */
    public Map<String, String> buildTmpTableFieldRefOriField(Set<String> fieldSet, String newTableAliasName){
        Map<String, String> refInfo = Maps.newConcurrentMap();
        for(String field : fieldSet){
            String[] fields = StringUtils.splitByWholeSeparator(field, "as");
            String oldKey = field;
            String[] oldFieldInfo = StringUtils.splitByWholeSeparator(fields[0], ".");
            String oldFieldName = oldFieldInfo.length == 2 ? oldFieldInfo[1] : oldFieldInfo[0];
            String newKey = fields.length == 2 ? newTableAliasName + "." + fields[1] :
                    newTableAliasName + "." + oldFieldName;
            refInfo.put(oldKey, newKey);
        }

        return refInfo;
    }


}
