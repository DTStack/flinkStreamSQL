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


    public Object parseSql(SqlNode sqlNode, Set<String> sideTableSet, Queue<Object> queueInfo, SqlNode parentWhere, SqlNodeList parentSelectList){
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
                JoinNodeDealer joinNodeDealer = new JoinNodeDealer(this);
                Set<Tuple2<String, String>> joinFieldSet = Sets.newHashSet();
                Map<String, String> tableRef = Maps.newHashMap();
                return joinNodeDealer.dealJoinNode((SqlJoin) sqlNode, sideTableSet, queueInfo, parentWhere, parentSelectList, joinFieldSet, tableRef);
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


    public void setLocalTableCache(Map<String, Table> localTableCache) {
        this.localTableCache = localTableCache;
    }

}
