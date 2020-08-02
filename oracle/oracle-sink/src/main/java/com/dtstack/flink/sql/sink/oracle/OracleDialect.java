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

package com.dtstack.flink.sql.sink.oracle;

import com.dtstack.flink.sql.sink.rdb.dialect.JDBCDialect;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.util.DtStringUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Date: 2020/1/3
 * Company: www.dtstack.com
 * @author maqi
 */
public class OracleDialect implements JDBCDialect {

    private final String SQL_DEFAULT_PLACEHOLDER = " ? ";
    private final String DEAL_CHAR_KEY = "char";
    private String RPAD_FORMAT = " rpad(?, %d, ' ') ";

    private List<String> fieldList;
    private List<String> fieldTypeList;
    private List<AbstractTableInfo.FieldExtraInfo> fieldExtraInfoList;

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:oracle:");
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("oracle.jdbc.driver.OracleDriver");
    }

    @Override
    public Optional<String> getUpsertStatement(String schema, String tableName, String[] fieldNames, String[] uniqueKeyFields, boolean allReplace) {
        tableName = DtStringUtil.getTableFullPath(schema, tableName);
        StringBuilder mergeIntoSql = new StringBuilder();
        mergeIntoSql.append("MERGE INTO " + tableName + " T1 USING (")
                .append(buildDualQueryStatement(fieldNames))
                .append(") T2 ON (")
                .append(buildConnectionConditions(uniqueKeyFields) + ") ");

        String updateSql = buildUpdateConnection(fieldNames, uniqueKeyFields, allReplace);

        if (StringUtils.isNotEmpty(updateSql)) {
            mergeIntoSql.append(" WHEN MATCHED THEN UPDATE SET ");
            mergeIntoSql.append(updateSql);
        }

        mergeIntoSql.append(" WHEN NOT MATCHED THEN ")
                .append("INSERT (")
                .append(Arrays.stream(fieldNames).map(this::quoteIdentifier).collect(Collectors.joining(",")))
                .append(") VALUES (")
                .append(Arrays.stream(fieldNames).map(col -> "T2." + quoteIdentifier(col)).collect(Collectors.joining(",")))
                .append(")");

        return Optional.of(mergeIntoSql.toString());
    }

    /**
     *   build   T1."A"=T2."A" or  T1."A"=nvl(T2."A",T1."A")
     * @param fieldNames
     * @param uniqueKeyFields
     * @param allReplace
     * @return
     */
    private String buildUpdateConnection(String[] fieldNames, String[] uniqueKeyFields, boolean allReplace) {
        List<String> uniqueKeyList = Arrays.asList(uniqueKeyFields);
        String updateConnectionSql = Arrays.stream(fieldNames).
                filter(col -> !uniqueKeyList.contains(col))
                .map(col -> buildConnectionByAllReplace(allReplace, col))
                .collect(Collectors.joining(","));
        return updateConnectionSql;
    }

    private String buildConnectionByAllReplace(boolean allReplace, String col) {
        String conncetionSql = allReplace ? quoteIdentifier("T1") + "." + quoteIdentifier(col) + " = " + quoteIdentifier("T2") + "." + quoteIdentifier(col) :
                quoteIdentifier("T1") + "." + quoteIdentifier(col) + " =nvl(" + quoteIdentifier("T2") + "." + quoteIdentifier(col) + ","
                        + quoteIdentifier("T1") + "." + quoteIdentifier(col) + ")";
        return conncetionSql;
    }


    private String buildConnectionConditions(String[] uniqueKeyFields) {
        return Arrays.stream(uniqueKeyFields).map(col -> "T1." + quoteIdentifier(col) + "=T2." + quoteIdentifier(col)).collect(Collectors.joining(","));
    }

    /**
     * build select sql , such as (SELECT ? "A",? "B" FROM DUAL)
     *
     * @param column   destination column
     * @return
     */
    public String buildDualQueryStatement(String[] column) {
        StringBuilder sb = new StringBuilder("SELECT ");
        String collect = Arrays.stream(column)
                .map(col -> wrapperPlaceholder(col) + quoteIdentifier(col))
                .collect(Collectors.joining(", "));
        sb.append(collect).append(" FROM DUAL");
        return sb.toString();
    }


    /**
     *  char type is wrapped with rpad
     * @param fieldName
     * @return
     */
    public String wrapperPlaceholder(String fieldName) {
        int pos = fieldList.indexOf(fieldName);
        String type = fieldTypeList.get(pos);

        if (StringUtils.contains(type.toLowerCase(), DEAL_CHAR_KEY)) {
            AbstractTableInfo.FieldExtraInfo fieldExtraInfo = fieldExtraInfoList.get(pos);
            int charLength = fieldExtraInfo == null ? 0 : fieldExtraInfo.getLength();
            if (charLength > 0) {
                return String.format(RPAD_FORMAT, charLength);
            }
        }
        return SQL_DEFAULT_PLACEHOLDER;
    }


    public void setFieldList(List<String> fieldList) {
        this.fieldList = fieldList;
    }

    public void setFieldTypeList(List<String> fieldTypeList) {
        this.fieldTypeList = fieldTypeList;
    }

    public void setFieldExtraInfoList(List<AbstractTableInfo.FieldExtraInfo> fieldExtraInfoList) {
        this.fieldExtraInfoList = fieldExtraInfoList;
    }
}
