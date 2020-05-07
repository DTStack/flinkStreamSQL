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

package com.dtstack.flink.sql.sink.db;

import com.dtstack.flink.sql.sink.rdb.dialect.JDBCDialect;
import com.dtstack.flink.sql.util.DtStringUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Date: 2020/1/19
 * Company: www.dtstack.com
 * @author maqi
 */
public class DbDialect implements JDBCDialect {
    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:db2:");
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.ibm.db2.jcc.DB2Driver");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return identifier;
    }

    @Override
    public Optional<String> getUpsertStatement(String schema, String tableName, String[] fieldNames, String[] uniqueKeyFields, boolean allReplace) {
        tableName = DtStringUtil.getTableFullPath(schema, tableName);
        StringBuilder sb = new StringBuilder();
        sb.append("MERGE INTO " + tableName + " T1 USING "
                + "(" + buildValuesStatement(fieldNames) + ") T2 ("
                + buildFiledNameStatement(fieldNames) +
                ") ON ("
                + buildConnectionConditions(uniqueKeyFields) + ") ");

        String updateSql = buildUpdateConnection(fieldNames, uniqueKeyFields, allReplace);

        if (StringUtils.isNotEmpty(updateSql)) {
            sb.append(" WHEN MATCHED THEN UPDATE SET ");
            sb.append(updateSql);
        }

        sb.append(" WHEN NOT MATCHED THEN "
                + "INSERT (" + Arrays.stream(fieldNames).map(this::quoteIdentifier).collect(Collectors.joining(",")) + ") VALUES ("
                + Arrays.stream(fieldNames).map(col -> "T2." + quoteIdentifier(col)).collect(Collectors.joining(",")) + ")");
        return Optional.of(sb.toString());
    }

    /**
     *   build    T1."A"=T2."A" or  T1."A"=nvl(T2."A",T1."A")
     * @param fieldNames
     * @param uniqueKeyFields
     * @param allReplace
     * @return
     */
    private String buildUpdateConnection(String[] fieldNames, String[] uniqueKeyFields, boolean allReplace) {
        List<String> uniqueKeyList = Arrays.asList(uniqueKeyFields);
        return Arrays.stream(fieldNames)
                .filter(col -> !uniqueKeyList.contains(col))
                .map(col -> buildConnectString(allReplace, col))
                .collect(Collectors.joining(","));
    }

    private String buildConnectString(boolean allReplace, String col) {
        return allReplace ? quoteIdentifier("T1") + "." + quoteIdentifier(col) + " = " + quoteIdentifier("T2") + "." + quoteIdentifier(col) :
                quoteIdentifier("T1") + "." + quoteIdentifier(col) + " =NVL(" + quoteIdentifier("T2") + "." + quoteIdentifier(col) + ","
                        + quoteIdentifier("T1") + "." + quoteIdentifier(col) + ")";
    }


    private String buildConnectionConditions(String[] uniqueKeyFields) {
        return Arrays.stream(uniqueKeyFields).map(col -> "T1." + quoteIdentifier(col) + "=T2." + quoteIdentifier(col)).collect(Collectors.joining(","));
    }

    /**
     * build sql part e.g:  VALUES('1001','zs','sss')
     *
     * @param column   destination column
     * @return
     */
    public String buildValuesStatement(String[] column) {
        StringBuilder sb = new StringBuilder("VALUES(");
        String collect = Arrays.stream(column)
                .map(col -> " ? ")
                .collect(Collectors.joining(", "));

        return sb.append(collect).append(")").toString();
    }

    /**
     * build sql part e.g:   id, name, address
     * @param column
     * @return
     */
    public String buildFiledNameStatement(String[] column) {
        return Arrays.stream(column)
                .collect(Collectors.joining(", "));
    }


}
