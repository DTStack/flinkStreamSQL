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
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final char SPLIT = '_';

    public Queue<Object> getExeQueue(String exeSql, Set<String> sideTableSet) throws SqlParseException {
        System.out.println("---exeSql---");
        System.out.println(exeSql);
        LOG.info("---exeSql---");
        LOG.info(exeSql);

        Queue<Object> queueInfo = Queues.newLinkedBlockingQueue();
        SqlParser sqlParser = SqlParser.create(exeSql, CalciteConfig.MYSQL_LEX_CONFIG);
        SqlNode sqlNode = sqlParser.parseStmt();

        parseSql(sqlNode, sideTableSet, queueInfo);
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


    private Object parseSql(SqlNode sqlNode, Set<String> sideTableSet, Queue<Object> queueInfo){
        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind){
            case WITH: {
                SqlWith sqlWith = (SqlWith) sqlNode;
                SqlNodeList sqlNodeList = sqlWith.withList;
                for (SqlNode withAsTable : sqlNodeList) {
                    SqlWithItem sqlWithItem = (SqlWithItem) withAsTable;
                    parseSql(sqlWithItem.query, sideTableSet, queueInfo);
                    queueInfo.add(sqlWithItem);
                }
                parseSql(sqlWith.body, sideTableSet, queueInfo);
                break;
            }
            case INSERT:
                SqlNode sqlSource = ((SqlInsert)sqlNode).getSource();
                return parseSql(sqlSource, sideTableSet, queueInfo);
            case SELECT:
                SqlNode sqlFrom = ((SqlSelect)sqlNode).getFrom();
                if(sqlFrom.getKind() != IDENTIFIER){
                    Object result = parseSql(sqlFrom, sideTableSet, queueInfo);
                    if(result instanceof JoinInfo){
                        return dealSelectResultWithJoinInfo((JoinInfo) result, (SqlSelect) sqlNode, queueInfo);
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
                return dealJoinNode((SqlJoin) sqlNode, sideTableSet, queueInfo);
            case AS:
                SqlNode info = ((SqlBasicCall)sqlNode).getOperands()[0];
                SqlNode alias = ((SqlBasicCall) sqlNode).getOperands()[1];
                String infoStr = "";

                if(info.getKind() == IDENTIFIER){
                    infoStr = info.toString();
                } else {
                    infoStr = parseSql(info, sideTableSet, queueInfo).toString();
                }

                AliasInfo aliasInfo = new AliasInfo();
                aliasInfo.setName(infoStr);
                aliasInfo.setAlias(alias.toString());

                return aliasInfo;

            case UNION:
                SqlNode unionLeft = ((SqlBasicCall)sqlNode).getOperands()[0];
                SqlNode unionRight = ((SqlBasicCall)sqlNode).getOperands()[1];

                parseSql(unionLeft, sideTableSet, queueInfo);
                parseSql(unionRight, sideTableSet, queueInfo);
                break;
            case ORDER_BY:
                SqlOrderBy sqlOrderBy  = (SqlOrderBy) sqlNode;
                parseSql(sqlOrderBy.query, sideTableSet, queueInfo);
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

        AliasInfo rightTableAliasInfo = getSqlNodeAliasInfo(sqlNode.getRight());
        if(sideTableSet.contains(rightTableAliasInfo.getName())){
            //构建新的查询
        }
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

    private JoinInfo dealJoinNode(SqlJoin joinNode, Set<String> sideTableSet, Queue<Object> queueInfo) {
        SqlNode leftNode = joinNode.getLeft();
        SqlNode rightNode = joinNode.getRight();
        JoinType joinType = joinNode.getJoinType();

        String leftTbName = "";
        String leftTbAlias = "";
        String rightTableName = "";
        String rightTableAlias = "";
        boolean leftTbisTmp = false;

        Tuple2<String, String> rightTableNameAndAlias = null;
        if(leftNode.getKind() == IDENTIFIER){
            leftTbName = leftNode.toString();
        } else if (leftNode.getKind() == JOIN) {
            //处理连续join
            SqlBasicCall sqlBasicCall = dealNestJoin((SqlJoin) leftNode, sideTableSet, queueInfo);
            leftTbName = sqlBasicCall.getOperands()[0].toString();
            leftTbAlias = sqlBasicCall.getOperands()[1].toString();
            leftTbisTmp = true;
        } else if (leftNode.getKind() == AS) {
            AliasInfo aliasInfo = (AliasInfo) parseSql(leftNode, sideTableSet, queueInfo);
            leftTbName = aliasInfo.getName();
            leftTbAlias = aliasInfo.getAlias();

        } else {
            throw new RuntimeException("---not deal---");
        }

        boolean leftIsSide = checkIsSideTable(leftTbName, sideTableSet);
        if(leftIsSide){
            throw new RuntimeException("side-table must be at the right of join operator");
        }

        rightTableNameAndAlias = parseRightNode(rightNode, sideTableSet, queueInfo);
        rightTableName = rightTableNameAndAlias.f0;
        rightTableAlias = rightTableNameAndAlias.f1;

        boolean rightIsSide = checkIsSideTable(rightTableName, sideTableSet);
        if(joinType == JoinType.RIGHT){
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
        return tableInfo;
    }

    //构建新的查询
    private SqlBasicCall dealNestJoin(SqlJoin joinNode, Set<String> sideTableSet, Queue<Object> queueInfo){
        SqlNode rightNode = joinNode.getRight();

        Tuple2<String, String> rightTableNameAndAlias = parseRightNode(rightNode, sideTableSet, queueInfo);
        String rightTableName = rightTableNameAndAlias.f0;
        boolean rightIsSide = checkIsSideTable(rightTableName, sideTableSet);

        if(!rightIsSide){
            return null;
        }

        JoinInfo joinInfo = dealJoinNode(joinNode, sideTableSet, queueInfo);
        queueInfo.offer(joinInfo);
        return buildAsNodeByJoinInfo(joinInfo, null, null);

    }

    private Tuple2<String, String> parseRightNode(SqlNode sqlNode, Set<String> sideTableSet, Queue<Object> queueInfo) {
        Tuple2<String, String> tabName = new Tuple2<>("", "");
        if(sqlNode.getKind() == IDENTIFIER){
            tabName.f0 = sqlNode.toString();
        }else{
            AliasInfo aliasInfo = (AliasInfo)parseSql(sqlNode, sideTableSet, queueInfo);
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


    /**
     *
     * @param joinInfo
     * @param sqlNode
     * @param queueInfo
     * @return   两个边关联后的新表表名
     */
    private String dealSelectResultWithJoinInfo(JoinInfo joinInfo, SqlSelect sqlNode, Queue<Object> queueInfo) {
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

    private void replaceFromNodeForJoin(JoinInfo joinInfo, SqlSelect sqlNode) {
        //Update from node
        SqlBasicCall sqlBasicCall = buildAsNodeByJoinInfo(joinInfo, null, null);
        sqlNode.setFrom(sqlBasicCall);
    }

    private SqlBasicCall buildAsNodeByJoinInfo(JoinInfo joinInfo, SqlNode sqlNode0, String tableAlias) {
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

    private String buildInternalTableName(String left, char split, String right) {
        StringBuilder sb = new StringBuilder();
        return sb.append(left).append(split).append(right).toString();
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
}
