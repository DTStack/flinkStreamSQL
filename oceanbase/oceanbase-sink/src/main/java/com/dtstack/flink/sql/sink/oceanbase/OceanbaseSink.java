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


package com.dtstack.flink.sql.sink.oceanbase;


import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.rdb.RdbSink;
import com.dtstack.flink.sql.sink.rdb.format.RetractJDBCOutputFormat;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Date: 2020/2/03
 * Company: www.dtstack.com
 * @author xiuzhu
 */

public class OceanbaseSink extends RdbSink implements IStreamSinkGener<RdbSink> {

    private static final String OB_DRIVER = "com.mysql.jdbc.Driver";

    public OceanbaseSink() {
    }

    @Override
    public void buildSql(String schema, String tableName, List<String> fields) {
        buildInsertSql(tableName, fields);
    }

    @Override
    public String buildUpdateSql(String schema, String tableName, List<String> fieldNames, Map<String, List<String>> realIndexes, List<String> fullField) {
        return null;
    }

    @Override
    public String getDriverName() {
        return OB_DRIVER;
    }

    @Override
    public RetractJDBCOutputFormat getOutputFormat() {
        return new RetractJDBCOutputFormat();
    }

    public void buildInsertSql(String tableName, List<String> fields) {
        String[] fieldsName = fields.toArray(new String[fields.size()]);

        String updateClause = Arrays.stream(fieldsName).map(f -> quoteIdentifier(f)
                + "=IFNULL(VALUES(" + quoteIdentifier(f) + ")," + quoteIdentifier(f) + ")")
                .collect(Collectors.joining(","));

        this.sql = getInsertIntoStatement("", tableName, fieldsName, null) +
                " ON DUPLICATE KEY UPDATE " + updateClause;
    }

    private String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    private String getInsertIntoStatement(String schema, String tableName, String[] fieldNames, String[] partitionFields) {
        String schemaInfo = StringUtils.isEmpty(schema) ? "" : quoteIdentifier(schema) + ".";
        String columns = Arrays.stream(fieldNames)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String placeholders = Arrays.stream(fieldNames)
                .map(f -> "?")
                .collect(Collectors.joining(", "));
        return "INSERT INTO " + schemaInfo + quoteIdentifier(tableName) +
                "(" + columns + ")" + " VALUES (" + placeholders + ")";
    }
}
