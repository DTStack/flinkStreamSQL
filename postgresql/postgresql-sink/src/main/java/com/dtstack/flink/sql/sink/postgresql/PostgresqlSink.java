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


package com.dtstack.flink.sql.sink.postgresql;


import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.postgresql.table.PostgresqlTableInfo;
import com.dtstack.flink.sql.sink.rdb.RdbSink;
import com.dtstack.flink.sql.sink.rdb.format.RetractJDBCOutputFormat;
import com.dtstack.flink.sql.table.TargetTableInfo;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * Date: 2019-08-22
 * Company: mmg
 *
 * @author tcm
 */

public class PostgresqlSink extends RdbSink implements IStreamSinkGener<RdbSink> {

    private static final String POSTGRESQL_DRIVER = "org.postgresql.Driver";

    private boolean isUpsert;

    private String keyField;

    public PostgresqlSink() {
    }

    @Override
    public RdbSink genStreamSink(TargetTableInfo targetTableInfo) {
        PostgresqlTableInfo pgTableInfo = (PostgresqlTableInfo) targetTableInfo;
        this.isUpsert = pgTableInfo.isUpsert();
        this.keyField = pgTableInfo.getKeyField();
        super.genStreamSink(targetTableInfo);
        return this;
    }

    @Override
    public RetractJDBCOutputFormat getOutputFormat() {
        return new RetractJDBCOutputFormat();
    }

    @Override
    public void buildSql(String scheam, String tableName, List<String> fields) {
        buildInsertSql(tableName, fields);
    }

    @Override
    public String buildUpdateSql(String schema, String tableName, List<String> fieldNames, Map<String, List<String>> realIndexes, List<String> fullField) {
        return null;
    }

    private void buildInsertSql(String tableName, List<String> fields) {
        StringBuffer sqlBuffer = new StringBuffer();

        sqlBuffer.append("insert into ".concat(tableName)
                .concat(" (")
                .concat(StringUtils.join(fields, ","))
                .concat(") ")
        );
        sqlBuffer.append("values (");
        StringBuffer upsertFields = new StringBuffer();
        for (String fieldName : fields) {
            sqlBuffer.append("?,");
            if (this.isUpsert) {
                if (fieldName.equals(this.keyField)) {
                    continue;
                }
                upsertFields.append(String.format("%s=excluded.%s,", fieldName, fieldName));
            }
        }
        sqlBuffer.deleteCharAt(sqlBuffer.length() - 1);
        sqlBuffer.append(")");

        if (this.isUpsert) {
            upsertFields.deleteCharAt(upsertFields.length() - 1);
            sqlBuffer.append(" ON conflict(".concat(keyField).concat(")"));
            sqlBuffer.append(" DO UPDATE SET ");
            sqlBuffer.append(upsertFields);
        }
        this.sql = sqlBuffer.toString();
    }

    @Override
    public String getDriverName() {
        return POSTGRESQL_DRIVER;
    }

}
