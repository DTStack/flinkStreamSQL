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

import com.dtstack.flink.sql.util.DtStringUtil;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.apache.flink.calcite.shaded.com.google.common.collect.Queues;

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

    public Queue<Object> getExeQueue(String exeSql, Set<String> sideTableSet) throws SqlParseException {
        System.out.println("---exeSql---");
        System.out.println(exeSql);
        Queue<Object> queueInfo = Queues.newLinkedBlockingQueue();
        SqlParser.Config config = SqlParser
                .configBuilder()
                .setLex(Lex.MYSQL)
                .build();
        SqlParser sqlParser = SqlParser.create(exeSql,config);
        SqlNode sqlNode = sqlParser.parseStmt();
        parseSql(sqlNode, sideTableSet, queueInfo);
        queueInfo.offer(sqlNode);
        return queueInfo;
    }

    private Object parseSql(SqlNode sqlNode, Set<String> sideTableSet, Queue<Object> queueInfo){
        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind){
            case INSERT:
                SqlNode sqlSource = ((SqlInsert)sqlNode).getSource();
                return parseSql(sqlSource, sideTableSet, queueInfo);
            case SELECT:
                SqlNode sqlFrom = ((SqlSelect)sqlNode).getFrom();
                if(sqlFrom.getKind() != IDENTIFIER){
                    Object result = parseSql(sqlFrom, sideTableSet, queueInfo);
                    if(result instanceof JoinInfo){
                        dealSelectResultWithJoinInfo((JoinInfo)result, (SqlSelect) sqlNode, queueInfo);
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
                String infoStr;

                if(info.getKind() == IDENTIFIER){
                    infoStr = info.toString();
                }else{
                    infoStr = parseSql(info, sideTableSet, queueInfo).toString();
                }

                AliasInfo aliasInfo = new AliasInfo();
                aliasInfo.setName(infoStr);
                aliasInfo.setAlias(alias.toString());

                return aliasInfo;

            case UNION:
                SqlNode unionLeft = ((SqlBasicCall) sqlNode).getOperands()[0];
                SqlNode unionRight = ((SqlBasicCall) sqlNode).getOperands()[1];

                parseSql(unionLeft, sideTableSet, queueInfo);

                parseSql(unionRight, sideTableSet, queueInfo);

                break;

            case ORDER_BY:
                SqlOrderBy sqlOrderBy  = (SqlOrderBy) sqlNode;
                parseSql(sqlOrderBy.query, sideTableSet, queueInfo);
        }

        return "";
    }

    private JoinInfo dealJoinNode(SqlJoin joinNode, Set<String> sideTableSet, Queue<Object> queueInfo){
        SqlNode leftNode = joinNode.getLeft();
        SqlNode rightNode = joinNode.getRight();
        JoinType joinType = joinNode.getJoinType();
        String leftTbName = "";
        String leftTbAlias = "";

        if(leftNode.getKind() == IDENTIFIER){
            leftTbName = leftNode.toString();
        }else if(leftNode.getKind() == JOIN){
            Object leftNodeJoinInfo = parseSql(leftNode, sideTableSet, queueInfo);
            System.out.println(leftNodeJoinInfo);
        }else if(leftNode.getKind() == AS){
            AliasInfo aliasInfo = (AliasInfo) parseSql(leftNode, sideTableSet, queueInfo);
            leftTbName = aliasInfo.getName();
            leftTbAlias = aliasInfo.getAlias();
        }else{
            throw new RuntimeException("---not deal---");
        }

        boolean leftIsSide = checkIsSideTable(leftTbName, sideTableSet);
        if(leftIsSide){
            throw new RuntimeException("side-table must be at the right of join operator");
        }

        String rightTableName = "";
        String rightTableAlias = "";

        if(rightNode.getKind() == IDENTIFIER){
            rightTableName = rightNode.toString();
        }else{
            AliasInfo aliasInfo = (AliasInfo)parseSql(rightNode, sideTableSet, queueInfo);
            rightTableName = aliasInfo.getName();
            rightTableAlias = aliasInfo.getAlias();
        }

        boolean rightIsSide = checkIsSideTable(rightTableName, sideTableSet);
        if(joinType == JoinType.RIGHT){
            throw new RuntimeException("side join not support join type of right[current support inner join and left join]");
        }

        JoinInfo tableInfo = new JoinInfo();
        tableInfo.setLeftTableName(leftTbName);
        tableInfo.setRightTableName(rightTableName);
        if (leftTbAlias.equals("")){
            tableInfo.setLeftTableAlias(leftTbName);
        } else {
            tableInfo.setLeftTableAlias(leftTbAlias);
        }
        if (leftTbAlias.equals("")){
            tableInfo.setRightTableAlias(rightTableName);
        } else {
            tableInfo.setRightTableAlias(rightTableAlias);
        }
        tableInfo.setLeftIsSideTable(leftIsSide);
        tableInfo.setRightIsSideTable(rightIsSide);
        tableInfo.setLeftNode(leftNode);
        tableInfo.setRightNode(rightNode);
        tableInfo.setJoinType(joinType);
        tableInfo.setCondition(joinNode.getCondition());

        return tableInfo;
    }


    private void dealSelectResultWithJoinInfo(JoinInfo joinInfo, SqlSelect sqlNode, Queue<Object> queueInfo){
        //SideJoinInfo rename
        if(joinInfo.checkIsSide()){
            joinInfo.setSelectFields(sqlNode.getSelectList());
            joinInfo.setSelectNode(sqlNode);
            if(joinInfo.isRightIsSideTable()){
                //Analyzing left is not a simple table
                if(joinInfo.getLeftNode().toString().contains("SELECT")){
                    queueInfo.offer(joinInfo.getLeftNode());
                }

                queueInfo.offer(joinInfo);
            }else{
                //Determining right is not a simple table
                if(joinInfo.getRightNode().getKind() == SELECT){
                    queueInfo.offer(joinInfo.getLeftNode());
                }

                queueInfo.offer(joinInfo);
            }

            //Update from node
            SqlOperator operator = new SqlAsOperator();
            SqlParserPos sqlParserPos = new SqlParserPos(0, 0);
            String joinLeftTableName = joinInfo.getLeftTableName();
            String joinLeftTableAlias = joinInfo.getLeftTableAlias();
            joinLeftTableName = Strings.isNullOrEmpty(joinLeftTableName) ? joinLeftTableAlias : joinLeftTableName;
            String newTableName = joinLeftTableName + "_" + joinInfo.getRightTableName();
            String newTableAlias = joinInfo.getLeftTableAlias() + "_" + joinInfo.getRightTableAlias();
            SqlIdentifier sqlIdentifier = new SqlIdentifier(newTableName, null, sqlParserPos);
            SqlIdentifier sqlIdentifierAlias = new SqlIdentifier(newTableAlias, null, sqlParserPos);
            SqlNode[] sqlNodes = new SqlNode[2];
            sqlNodes[0] = sqlIdentifier;
            sqlNodes[1] = sqlIdentifierAlias;
            SqlBasicCall sqlBasicCall = new SqlBasicCall(operator, sqlNodes, sqlParserPos);
            sqlNode.setFrom(sqlBasicCall);
        }
    }

    private boolean checkIsSideTable(String tableName, Set<String> sideTableList){
        if(sideTableList.contains(tableName)){
            return true;
        }

        return false;
    }
}
