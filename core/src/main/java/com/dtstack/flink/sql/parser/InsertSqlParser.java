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

import com.dtstack.flink.sql.enums.PlannerType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;
import com.google.common.collect.Lists;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;

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

    private FlinkPlanner flinkPlanner = new FlinkPlanner();

    // 用来标识当前解析节点的上一层节点是否为 insert 节点
    private static Boolean parentIsInsert = false;

    @Override
    public boolean verify(String sql) {
        return StringUtils.isNotBlank(sql) && sql.trim().toLowerCase().startsWith("insert");
    }

    public static InsertSqlParser newInstance(){
        InsertSqlParser parser = new InsertSqlParser();
        return parser;
    }

    @Override
    public void parseSql(String sql, SqlTree sqlTree, String planner) throws Exception {


        SqlNode sqlNode = flinkPlanner.getParser().parse(sql);

        SqlParseResult sqlParseResult = new SqlParseResult();
        parseNode(sqlNode, sqlParseResult);
        if (planner.equalsIgnoreCase(PlannerType.FLINK.name())) {
            sqlParseResult.setExecSql(sql);
        } else {
            sqlParseResult.setExecSql(sqlNode.toString());
        }
        sqlTree.addExecSql(sqlParseResult);
    }

    private static void parseNode(SqlNode sqlNode, SqlParseResult sqlParseResult){
        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind){
            case INSERT:
                SqlNode sqlTarget = ((SqlInsert)sqlNode).getTargetTable();
                SqlNode sqlSource = ((SqlInsert)sqlNode).getSource();
                sqlParseResult.addTargetTable(sqlTarget.toString());
                parentIsInsert = true;
                parseNode(sqlSource, sqlParseResult);
                break;
            case SELECT:
                SqlSelect sqlSelect = (SqlSelect) sqlNode;
                if (parentIsInsert) {
                    rebuildSelectNode(sqlSelect.getSelectList(), sqlSelect);
                }
                SqlNode sqlFrom = ((SqlSelect) sqlNode).getFrom();
                if (sqlFrom.getKind() == IDENTIFIER) {
                    sqlParseResult.addSourceTable(sqlFrom.toString());
                } else {
                    parentIsInsert = false;
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
            case SNAPSHOT:
                SqlSnapshot sqlSnapshot = (SqlSnapshot) sqlNode;
                sqlParseResult.addSourceTable(sqlSnapshot.getTableRef().toString());
                break;
            default:
                //do nothing
                break;
        }
    }

    /**
     * 将第一层 select 中的 sqlNode 转化为 AsNode，解决字段名冲突问题
     * 仅对 table.xx 这种类型的字段进行替换
     * @param selectList select Node 的 select 字段
     * @param sqlSelect 第一层解析出来的 selectNode
     */
    private static void rebuildSelectNode(SqlNodeList selectList, SqlSelect sqlSelect) {
        SqlNodeList sqlNodes = new SqlNodeList(selectList.getParserPosition());

        for (int index = 0; index < selectList.size(); index++) {
            if (selectList.get(index).getKind().equals(SqlKind.AS)
                    || ((SqlIdentifier) selectList.get(index)).names.size() == 1) {
                sqlNodes.add(selectList.get(index));
                continue;
            }
            sqlNodes.add(transformToAsNode(selectList.get(index)));
        }
        sqlSelect.setSelectList(sqlNodes);
    }

    /**
     * 将 sqlNode 转化为 AsNode
     * @param sqlNode 需要转化的 sqlNode
     * @return 重新构造的 AsNode
     */
    public static SqlBasicCall transformToAsNode(SqlNode sqlNode) {
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
