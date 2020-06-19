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

 

package com.dtstack.flink.sql.parser;

import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;
import com.google.common.collect.Lists;
import org.apache.flink.table.calcite.FlinkPlannerImpl;

import java.util.List;

import static org.apache.calcite.sql.SqlKind.IDENTIFIER;

/**
 * 解析flink sql
 * sql 只支持 insert 开头的
 * Date: 2018/6/22
 * Company: www.dtstack.com
 * @author xuchao
 */

public class InsertSqlParser implements IParser {

    @Override
    public boolean verify(String sql) {
        return StringUtils.isNotBlank(sql) && sql.trim().toLowerCase().startsWith("insert");
    }

    public static InsertSqlParser newInstance(){
        InsertSqlParser parser = new InsertSqlParser();
        return parser;
    }

    @Override
    public void parseSql(String sql, SqlTree sqlTree) {

        FlinkPlannerImpl flinkPlanner = FlinkPlanner.getFlinkPlanner();
        SqlNode sqlNode = flinkPlanner.parse(sql);

        SqlParseResult sqlParseResult = new SqlParseResult();
        parseNode(sqlNode, sqlParseResult);
        sqlParseResult.setExecSql(sqlNode.toString());
        sqlTree.addExecSql(sqlParseResult);
    }

    private static void parseNode(SqlNode sqlNode, SqlParseResult sqlParseResult){
        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind){
            case INSERT:
                SqlNode sqlTarget = ((SqlInsert)sqlNode).getTargetTable();
                SqlNode sqlSource = ((SqlInsert)sqlNode).getSource();
                sqlParseResult.addTargetTable(sqlTarget.toString());
                parseNode(sqlSource, sqlParseResult);
                break;
            case SELECT:
                SqlSelect sqlSelect = (SqlSelect) sqlNode;
                SqlNodeList selectList = sqlSelect.getSelectList();
                SqlNodeList sqlNodes = new SqlNodeList(selectList.getParserPosition());
                for (int index = 0; index < selectList.size(); index++) {
                    if (selectList.get(index).getKind().equals(SqlKind.AS)) {
                        sqlNodes.add(selectList.get(index));
                        continue;
                    }
                    sqlNodes.add(transformToSqlBasicCall(selectList.get(index)));
                }
                sqlSelect.setSelectList(sqlNodes);
                SqlNode sqlFrom = ((SqlSelect) sqlNode).getFrom();
                if (sqlFrom.getKind() == IDENTIFIER) {
                    sqlParseResult.addSourceTable(sqlFrom.toString());
                } else {
                    parseNode(sqlFrom, sqlParseResult);
                }
                break;
            case JOIN:
                SqlNode leftNode = ((SqlJoin)sqlNode).getLeft();
                SqlNode rightNode = ((SqlJoin)sqlNode).getRight();

                if(leftNode.getKind() == IDENTIFIER){
                    sqlParseResult.addSourceTable(leftNode.toString());
                }else{
                    parseNode(leftNode, sqlParseResult);
                }

                if(rightNode.getKind() == IDENTIFIER){
                    sqlParseResult.addSourceTable(rightNode.toString());
                }else{
                    parseNode(rightNode, sqlParseResult);
                }
                break;
            case AS:
                //不解析column,所以 as 相关的都是表
                SqlNode identifierNode = ((SqlBasicCall)sqlNode).getOperands()[0];
                if(identifierNode.getKind() != IDENTIFIER){
                    parseNode(identifierNode, sqlParseResult);
                }else {
                    sqlParseResult.addSourceTable(identifierNode.toString());
                }
                break;
            case MATCH_RECOGNIZE:
                SqlMatchRecognize node = (SqlMatchRecognize) sqlNode;
                sqlParseResult.addSourceTable(node.getTableRef().toString());
                break;
            case UNION:
                SqlNode unionLeft = ((SqlBasicCall)sqlNode).getOperands()[0];
                SqlNode unionRight = ((SqlBasicCall)sqlNode).getOperands()[1];
                if(unionLeft.getKind() == IDENTIFIER){
                    sqlParseResult.addSourceTable(unionLeft.toString());
                }else{
                    parseNode(unionLeft, sqlParseResult);
                }
                if(unionRight.getKind() == IDENTIFIER){
                    sqlParseResult.addSourceTable(unionRight.toString());
                }else{
                    parseNode(unionRight, sqlParseResult);
                }
                break;
            case ORDER_BY:
                SqlOrderBy sqlOrderBy  = (SqlOrderBy) sqlNode;
                parseNode(sqlOrderBy.query, sqlParseResult);
                break;
            default:
                //do nothing
                break;
        }
    }

    // 将 sqlNode 转换为 SqlBasicCall
    public static SqlBasicCall transformToSqlBasicCall(SqlNode sqlNode) {
        String asName = "";
        SqlParserPos pos = new SqlParserPos(sqlNode.getParserPosition().getLineNum(),
                                            sqlNode.getParserPosition().getEndColumnNum());
        if (sqlNode.getKind().equals(SqlKind.IDENTIFIER)) {
            asName = ((SqlIdentifier) sqlNode).names.get(1);
        }
        SqlNode[] operands = new SqlNode[2];
        operands[0] = sqlNode;
        operands[1] = new SqlIdentifier(asName, null, pos);
        return new SqlBasicCall(new SqlAsOperator(), operands, pos);
    }

    public static class SqlParseResult {

        private List<String> sourceTableList = Lists.newArrayList();

        private List<String> targetTableList = Lists.newArrayList();

        private String execSql;

        public void addSourceTable(String sourceTable){
            sourceTableList.add(sourceTable);
        }

        public void addTargetTable(String targetTable){
            targetTableList.add(targetTable);
        }

        public List<String> getSourceTableList() {
            return sourceTableList;
        }

        public List<String> getTargetTableList() {
            return targetTableList;
        }

        public String getExecSql() {
            return execSql;
        }

        public void setExecSql(String execSql) {
            this.execSql = execSql;
        }
    }
}
