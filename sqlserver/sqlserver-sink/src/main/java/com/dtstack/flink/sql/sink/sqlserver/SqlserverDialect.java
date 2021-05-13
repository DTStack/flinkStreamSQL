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

package com.dtstack.flink.sql.sink.sqlserver;

import com.dtstack.flink.sql.sink.rdb.dialect.JDBCDialect;
import com.dtstack.flink.sql.util.DtStringUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Date: 2020/1/15
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class SqlserverDialect implements JDBCDialect {

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:sqlserver:");
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    }

    @Override
    public Optional<String> getUpsertStatement(String schema, String tableName, String[] fieldNames, String[] uniqueKeyFields, boolean allReplace) {
        tableName = DtStringUtil.getTableFullPath(schema, tableName);
        StringBuilder sb = new StringBuilder();
        sb.append("MERGE INTO ")
            .append(tableName)
            .append(" T1 USING (")
            .append(buildDualQueryStatement(fieldNames))
            .append(") T2 ON (")
            .append(buildConnectionConditions(uniqueKeyFields))
            .append(") ");

        String updateSql = buildUpdateConnection(fieldNames, uniqueKeyFields, allReplace);

        if (StringUtils.isNotEmpty(updateSql)) {
            sb.append(" WHEN MATCHED THEN UPDATE SET ")
                .append(updateSql);
        }

        sb.append(" WHEN NOT MATCHED THEN " + "INSERT (")
            .append(
                Arrays
                    .stream(fieldNames)
                    .map(this::quoteIdentifier)
                    .collect(Collectors.joining(",")))
            .append(") VALUES (")
            .append(
                Arrays
                    .stream(fieldNames)
                    .map(col -> "T2." + quoteIdentifier(col))
                    .collect(Collectors.joining(",")))
            .append(");");
        return Optional.of(sb.toString());
    }

    /**
     * build    T1."A"=T2."A" or  T1."A"=nvl(T2."A",T1."A")
     *
     * @param fieldNames
     * @param uniqueKeyFields
     * @param allReplace
     * @return
     */
    private String buildUpdateConnection(String[] fieldNames, String[] uniqueKeyFields, boolean allReplace) {
        List<String> uniqueKeyList = Arrays.asList(uniqueKeyFields);
        return
            Arrays
                .stream(fieldNames)
                .filter(col -> !uniqueKeyList.contains(col))
                .map(col -> allReplace ?
                    quoteIdentifier("T1") + "." + quoteIdentifier(col) + " = " + quoteIdentifier("T2") + "." + quoteIdentifier(col) :
                    quoteIdentifier("T1") + "." + quoteIdentifier(col) + " =ISNULL(" + quoteIdentifier("T2") + "." + quoteIdentifier(col)
                        + "," + quoteIdentifier("T1") + "." + quoteIdentifier(col) + ")").collect(Collectors.joining(","));
    }


    private String buildConnectionConditions(String[] uniqueKeyFields) {
        return
            Arrays
                .stream(uniqueKeyFields)
                .map(col -> "T1." + quoteIdentifier(col) + "=T2." + quoteIdentifier(col))
                .collect(Collectors.joining(","));
    }

    /**
     * build select sql , such as (SELECT ? "A",? "B" FROM DUAL)
     *
     * @param column destination column
     * @return
     */
    public String buildDualQueryStatement(String[] column) {
        StringBuilder sb = new StringBuilder("SELECT ");
        String collect =
            Arrays
                .stream(column)
                .map(col -> " ? " + quoteIdentifier(col))
                .collect(Collectors.joining(", "));
        sb.append(collect);
        return sb.toString();
    }
}
