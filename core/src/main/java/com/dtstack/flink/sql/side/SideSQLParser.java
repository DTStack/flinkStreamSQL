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
import com.dtstack.flink.sql.util.TableUtils;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static org.apache.calcite.sql.SqlKind.IDENTIFIER;

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
        LOG.info("----------exec original Sql----------");
        LOG.info(exeSql);

        Queue<Object> queueInfo = Queues.newLinkedBlockingQueue();
        FlinkPlannerImpl flinkPlanner = FlinkPlanner.getFlinkPlanner();
        SqlNode sqlNode = flinkPlanner.parse(exeSql);

        parseSql(sqlNode, sideTableSet, queueInfo, null, null, null);
        queueInfo.offer(sqlNode);
        return queueInfo;
    }


    /**
     *  解析 sql 根据维表 join关系重新组装新的sql
     * @param sqlNode
     * @param sideTableSet
     * @param queueInfo
     * @param parentWhere
     * @param parentSelectList
     * @return
     */
    public Object parseSql(SqlNode sqlNode,
                           Set<String> sideTableSet,
                           Queue<Object> queueInfo,
                           SqlNode parentWhere,
                           SqlNodeList parentSelectList,
                           SqlNodeList parentGroupByList){
        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind){
            case WITH: {
                SqlWith sqlWith = (SqlWith) sqlNode;
                SqlNodeList sqlNodeList = sqlWith.withList;
                for (SqlNode withAsTable : sqlNodeList) {
                    SqlWithItem sqlWithItem = (SqlWithItem) withAsTable;
                    parseSql(sqlWithItem.query, sideTableSet, queueInfo, parentWhere, parentSelectList, parentGroupByList);
                    queueInfo.add(sqlWithItem);
                }
                parseSql(sqlWith.body, sideTableSet, queueInfo, parentWhere, parentSelectList, parentGroupByList);
                break;
            }
            case INSERT:
                SqlNode sqlSource = ((SqlInsert)sqlNode).getSource();
                return parseSql(sqlSource, sideTableSet, queueInfo, parentWhere, parentSelectList, parentGroupByList);
            case SELECT:
                SqlNode sqlFrom = ((SqlSelect)sqlNode).getFrom();
                SqlNode sqlWhere = ((SqlSelect)sqlNode).getWhere();
                SqlNodeList selectList = ((SqlSelect)sqlNode).getSelectList();
                SqlNodeList groupByList = ((SqlSelect) sqlNode).getGroup();

                if(sqlFrom.getKind() != IDENTIFIER){
                    Object result = parseSql(sqlFrom, sideTableSet, queueInfo, sqlWhere, selectList, groupByList);
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
                Map<String, String> fieldRef = Maps.newHashMap();
                return joinNodeDealer.dealJoinNode((SqlJoin) sqlNode, sideTableSet, queueInfo,
                        parentWhere, parentSelectList, parentGroupByList, joinFieldSet, tableRef, fieldRef);
            case AS:
                SqlNode info = ((SqlBasicCall)sqlNode).getOperands()[0];
                SqlNode alias = ((SqlBasicCall) sqlNode).getOperands()[1];
                String infoStr = "";

                if(info.getKind() == IDENTIFIER){
                    infoStr = info.toString();
                } else {
                    infoStr = parseSql(info, sideTableSet, queueInfo, parentWhere, parentSelectList, parentGroupByList).toString();
                }

                AliasInfo aliasInfo = new AliasInfo();
                aliasInfo.setName(infoStr);
                aliasInfo.setAlias(alias.toString());

                return aliasInfo;

            case UNION:
                SqlNode unionLeft = ((SqlBasicCall)sqlNode).getOperands()[0];
                SqlNode unionRight = ((SqlBasicCall)sqlNode).getOperands()[1];

                parseSql(unionLeft, sideTableSet, queueInfo, parentWhere, parentSelectList, parentGroupByList);
                parseSql(unionRight, sideTableSet, queueInfo, parentWhere, parentSelectList, parentGroupByList);
                break;
            case ORDER_BY:
                SqlOrderBy sqlOrderBy  = (SqlOrderBy) sqlNode;
                parseSql(sqlOrderBy.query, sideTableSet, queueInfo, parentWhere, parentSelectList, parentGroupByList);

            case LITERAL:
                return LITERAL.toString();
            default:
                break;
        }
        return "";
    }




    public void setLocalTableCache(Map<String, Table> localTableCache) {
        this.localTableCache = localTableCache;
    }

}
