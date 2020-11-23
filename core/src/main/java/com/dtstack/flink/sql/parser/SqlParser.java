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

import com.dtstack.flink.sql.enums.ETableType;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.table.AbstractTableInfoParser;
import com.dtstack.flink.sql.util.DtStringUtil;
import org.apache.commons.lang3.StringUtils;
import com.google.common.collect.Lists;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reason:
 * Date: 2018/6/22
 * Company: www.dtstack.com
 * @author xuchao
 */

public class SqlParser {
    private static final Logger LOG = LoggerFactory.getLogger(SqlParser.class);

    private static final char SQL_DELIMITER = ';';

    private static String LOCAL_SQL_PLUGIN_ROOT;

    private static List<IParser> sqlParserList = Lists.newArrayList(CreateFuncParser.newInstance(),
            CreateTableParser.newInstance(), InsertSqlParser.newInstance(), CreateTmpTableParser.newInstance());

    public static void setLocalSqlPluginRoot(String localSqlPluginRoot){
        LOCAL_SQL_PLUGIN_ROOT = localSqlPluginRoot;
    }

    private static final Pattern ADD_FILE_AND_JAR_PATTERN = Pattern.compile("(?i).*add\\s+file\\s+.+|(?i).*add\\s+jar\\s+.+");

    /**
     * flink support sql syntax
     * CREATE TABLE sls_stream() with ();
     * CREATE (TABLE|SCALA) FUNCTION fcnName WITH com.dtstack.com;
     * insert into tb1 select * from tb2;
     * @param sql
     */
    public static SqlTree parseSql(String sql, String pluginLoadMode, String planner) throws Exception {

        if(StringUtils.isBlank(sql)){
            throw new RuntimeException("sql is not null");
        }

        sql = DtStringUtil.dealSqlComment(sql)
                .replaceAll("\r\n", " ")
                .replaceAll("\n", " ")
                .replace("\t", " ").trim();

        List<String> sqlArr = DtStringUtil.splitIgnoreQuota(sql, SQL_DELIMITER);
        sqlArr = removeAddFileAndJarStmt(sqlArr);
        SqlTree sqlTree = new SqlTree();
        AbstractTableInfoParser tableInfoParser = new AbstractTableInfoParser();
        for(String childSql : sqlArr){
            if(Strings.isNullOrEmpty(childSql)){
                continue;
            }
            boolean result = false;
            for(IParser sqlParser : sqlParserList){
                try {
                    if (!sqlParser.verify(childSql)) {
                        continue;
                    }

                    sqlParser.parseSql(childSql, sqlTree, planner);
                    result = true;
                    break;
                } catch (Exception e) {
                    LOG.error("'{}' parser error, detail info: {}", childSql, e.getMessage(), e);
                    throw e;
                }
            }

            if(!result){
                throw new RuntimeException(String.format("%s:Syntax does not support,the format of SQL like insert into tb1 select * from tb2.", childSql));
            }
        }

        //解析exec-sql
        if(sqlTree.getExecSqlList().size() == 0){
            throw new RuntimeException("sql no executable statement");
        }

        for(InsertSqlParser.SqlParseResult result : sqlTree.getExecSqlList()){
            List<String> sourceTableList = result.getSourceTableList();
            List<String> targetTableList = result.getTargetTableList();
            Set<String> tmpTableList = sqlTree.getTmpTableMap().keySet();

            for(String tableName : sourceTableList){
                if (!tmpTableList.contains(tableName)){
                    CreateTableParser.SqlParserResult createTableResult = sqlTree.getPreDealTableMap().get(tableName);
                    if(createTableResult == null){
                        throw new RuntimeException("can't find table " + tableName);
                    }

                    AbstractTableInfo tableInfo = tableInfoParser.parseWithTableType(ETableType.SOURCE.getType(),
                            createTableResult, LOCAL_SQL_PLUGIN_ROOT, pluginLoadMode, planner);
                    sqlTree.addTableInfo(tableName, tableInfo);
                }
            }

            for(String tableName : targetTableList){
                if (!tmpTableList.contains(tableName)){
                    CreateTableParser.SqlParserResult createTableResult = sqlTree.getPreDealTableMap().get(tableName);
                    if(createTableResult == null){
                        throw new RuntimeException("can't find table " + tableName);
                    }

                    AbstractTableInfo tableInfo = tableInfoParser.parseWithTableType(ETableType.SINK.getType(),
                            createTableResult, LOCAL_SQL_PLUGIN_ROOT, pluginLoadMode, planner);
                    sqlTree.addTableInfo(tableName, tableInfo);
                }
            }
        }

        for (CreateTmpTableParser.SqlParserResult result : sqlTree.getTmpSqlList()){
            List<String> sourceTableList = result.getSourceTableList();
            for(String tableName : sourceTableList){
                if (!sqlTree.getTableInfoMap().keySet().contains(tableName)){
                    CreateTableParser.SqlParserResult createTableResult = sqlTree.getPreDealTableMap().get(tableName);
                    if(createTableResult == null){
                        CreateTmpTableParser.SqlParserResult tmpTableResult = sqlTree.getTmpTableMap().get(tableName);
                        if (tmpTableResult == null){
                            throw new RuntimeException("can't find table " + tableName);
                        }
                    } else {
                        AbstractTableInfo tableInfo = tableInfoParser.parseWithTableType(ETableType.SOURCE.getType(),
                                createTableResult, LOCAL_SQL_PLUGIN_ROOT, pluginLoadMode, planner);
                        sqlTree.addTableInfo(tableName, tableInfo);
                    }
                }
            }
        }

        return sqlTree;
    }

    /**
     * remove add file and jar with statment etc. add file /etc/krb5.conf, add jar xxx.jar;
     */
    private static List<String> removeAddFileAndJarStmt(List<String> stmts) {
        List<String> cleanedStmts = Lists.newArrayList();
        for (String stmt : stmts) {
            Matcher matcher = ADD_FILE_AND_JAR_PATTERN.matcher(stmt);
            if(!matcher.matches()) {
                cleanedStmts.add(stmt);
            }
        }
        return cleanedStmts;
    }
}
