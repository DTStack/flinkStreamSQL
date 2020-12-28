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
import com.dtstack.flink.sql.util.DtStringUtil;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSnapshot;
import org.apache.calcite.sql.SqlSelect;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.calcite.sql.SqlKind.IDENTIFIER;

/**
 * parser create tmp table sql
 * Date: 2018/6/26
 * Company: www.dtstack.com
 *
 * @author yanxi
 */
public class CreateTmpTableParser implements IParser {

    //select table tableName as select
    private static final String TEMPORARY_STR = "(?i)create\\s+(temporary\\s+)?view\\s+(if\\s+not\\s+exists\\s+)?([^\\s]+)\\s+as\\s+select\\s+(.*)";

    private static final String EMPTY_STR = "(?i)^\\screate\\s+view\\s+(\\S+)\\s*\\((.+)\\)$";

    private static final Pattern TEMPORARYVIEW = Pattern.compile(TEMPORARY_STR);

    private static final Pattern EMPTYVIEW = Pattern.compile(EMPTY_STR);

    private final FlinkPlanner flinkPlanner = new FlinkPlanner();

    public static CreateTmpTableParser newInstance() {
        return new CreateTmpTableParser();
    }

    private static void parseNode(SqlNode sqlNode, CreateTmpTableParser.SqlParserResult sqlParseResult) {
        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind) {
            case SELECT:
                SqlNode sqlFrom = ((SqlSelect) sqlNode).getFrom();
                if (sqlFrom.getKind() == IDENTIFIER) {
                    sqlParseResult.addSourceTable(sqlFrom.toString());
                } else {
                    parseNode(sqlFrom, sqlParseResult);
                }
                break;
            case JOIN:
                SqlNode leftNode = ((SqlJoin) sqlNode).getLeft();
                SqlNode rightNode = ((SqlJoin) sqlNode).getRight();

                if (leftNode.getKind() == IDENTIFIER) {
                    sqlParseResult.addSourceTable(leftNode.toString());
                } else {
                    parseNode(leftNode, sqlParseResult);
                }

                if (rightNode.getKind() == IDENTIFIER) {
                    sqlParseResult.addSourceTable(rightNode.toString());
                } else {
                    parseNode(rightNode, sqlParseResult);
                }
                break;
            case AS:
                //不解析column,所以 as 相关的都是表
                SqlNode identifierNode = ((SqlBasicCall) sqlNode).getOperands()[0];
                if (identifierNode.getKind() != IDENTIFIER) {
                    parseNode(identifierNode, sqlParseResult);
                } else {
                    sqlParseResult.addSourceTable(identifierNode.toString());
                }
                break;
            case UNION:
                SqlNode unionLeft = ((SqlBasicCall) sqlNode).getOperands()[0];
                SqlNode unionRight = ((SqlBasicCall) sqlNode).getOperands()[1];
                if (unionLeft.getKind() == IDENTIFIER) {
                    sqlParseResult.addSourceTable(unionLeft.toString());
                } else {
                    parseNode(unionLeft, sqlParseResult);
                }
                if (unionRight.getKind() == IDENTIFIER) {
                    sqlParseResult.addSourceTable(unionRight.toString());
                } else {
                    parseNode(unionRight, sqlParseResult);
                }
                break;
            case MATCH_RECOGNIZE:
                SqlMatchRecognize node = (SqlMatchRecognize) sqlNode;
                sqlParseResult.addSourceTable(node.getTableRef().toString());
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

    @Override
    public boolean verify(String sql) {
        if (Pattern.compile(EMPTY_STR).matcher(sql).find()) {
            return true;
        }
        return NONEMPTYVIEW.matcher(sql).find();
    }

    @Override
    public void parseSql(String sql, SqlTree sqlTree) {
        if (NONEMPTYVIEW.matcher(sql).find()) {
            Matcher matcher = NONEMPTYVIEW.matcher(sql);
            String tableName = null;
            String selectSql = null;
            if (matcher.find()) {
                tableName = matcher.group(1);
                selectSql = "select " + matcher.group(2);
            }

            SqlNode sqlNode = null;
            try {
                sqlNode = flinkPlanner.getParser().parse(selectSql);
            } catch (Exception e) {
                throw new RuntimeException("", e);
            }

            CreateTmpTableParser.SqlParserResult sqlParseResult = new CreateTmpTableParser.SqlParserResult();
            parseNode(sqlNode, sqlParseResult);

            sqlParseResult.setTableName(tableName);
            String transformSelectSql = DtStringUtil.replaceIgnoreQuota(sqlNode.toString(), "`", "");
            sqlParseResult.setExecSql(transformSelectSql);
            sqlTree.addTmpSql(sqlParseResult);
            sqlTree.addTmplTableInfo(tableName, sqlParseResult);
        } else {
            if (EMPTYVIEW.matcher(sql).find()) {
                Matcher matcher = EMPTYVIEW.matcher(sql);
                String tableName = null;
                String fieldsInfoStr = null;
                if (matcher.find()) {
                    tableName = matcher.group(1);
                    fieldsInfoStr = matcher.group(2);
                }
                CreateTmpTableParser.SqlParserResult sqlParseResult = new CreateTmpTableParser.SqlParserResult();
                sqlParseResult.setFieldsInfoStr(fieldsInfoStr);
                sqlParseResult.setTableName(tableName);
                sqlTree.addTmplTableInfo(tableName, sqlParseResult);
            }
        }
    }

    public static class SqlParserResult {
        private String tableName;

        private String fieldsInfoStr;

        private String execSql;

        private List<String> sourceTableList = Lists.newArrayList();

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getExecSql() {
            return execSql;
        }

        public void setExecSql(String execSql) {
            this.execSql = execSql;
        }

        public String getFieldsInfoStr() {
            return fieldsInfoStr;
        }

        public void setFieldsInfoStr(String fieldsInfoStr) {
            this.fieldsInfoStr = fieldsInfoStr;
        }

        public void addSourceTable(String sourceTable) {
            sourceTableList.add(sourceTable);
        }

        public List<String> getSourceTableList() {
            return sourceTableList;
        }
    }
}