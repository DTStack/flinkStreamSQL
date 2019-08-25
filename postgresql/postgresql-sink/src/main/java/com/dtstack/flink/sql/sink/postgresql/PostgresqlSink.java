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
import com.dtstack.flink.sql.sink.postgresql.table.PgTableInfo;
import com.dtstack.flink.sql.sink.rdb.RdbSink;
import com.dtstack.flink.sql.sink.rdb.format.RetractJDBCOutputFormat;
import com.dtstack.flink.sql.sink.rdb.table.RdbTableInfo;
import com.dtstack.flink.sql.table.TargetTableInfo;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Date: 2019-08-22
 * Company: mmg
 *
 * @author tcm
 */

public class PostgresqlSink extends RdbSink implements IStreamSinkGener<RdbSink> {

    //支持Cockroach数据库
    protected boolean isCockroach;
    protected String keyField;
    boolean isHasKey;

    private static final String POSTGRESQL_DRIVER = "org.postgresql.Driver";

    public PostgresqlSink() {
    }

    @Override
    public PostgresqlSink genStreamSink(TargetTableInfo targetTableInfo) {
        //super.genStreamSink(targetTableInfo);

        PgTableInfo rdbTableInfo = (PgTableInfo) targetTableInfo;

        //PgTableInfo pgTableInfo = (PgTableInfo) targetTableInfo;
        this.isCockroach = rdbTableInfo.isCockroach();
        this.keyField = rdbTableInfo.getKeyField();
        if (StringUtils.isEmpty(this.keyField)) {
            isHasKey = false;
        } else {
            isHasKey = true;
        }

        String tmpDbURL = rdbTableInfo.getUrl();
        String tmpUserName = rdbTableInfo.getUserName();
        String tmpPassword = rdbTableInfo.getPassword();
        String tmpTableName = rdbTableInfo.getTableName();

        Integer tmpSqlBatchSize = rdbTableInfo.getBatchSize();
        if (tmpSqlBatchSize != null) {
            setBatchNum(tmpSqlBatchSize);
        }

        Long batchWaitInterval = rdbTableInfo.getBatchWaitInterval();
        if (batchWaitInterval != null) {
            setBatchWaitInterval(batchWaitInterval);
        }

        Integer tmpSinkParallelism = rdbTableInfo.getParallelism();
        if (tmpSinkParallelism != null) {
            setParallelism(tmpSinkParallelism);
        }

        List<String> fields = Arrays.asList(rdbTableInfo.getFields());
        List<Class> fieldTypeArray = Arrays.asList(rdbTableInfo.getFieldClasses());

        this.driverName = getDriverName();
        this.dbURL = tmpDbURL;
        this.userName = tmpUserName;
        this.password = tmpPassword;
        this.tableName = tmpTableName;
        this.primaryKeys = rdbTableInfo.getPrimaryKeys();
        this.dbType = rdbTableInfo.getType();

        buildSql(tableName, fields);
        buildSqlTypes(fieldTypeArray);

        return this;
    }

    @Override
    public RetractJDBCOutputFormat getOutputFormat() {
        return new RetractJDBCOutputFormat();
    }

    @Override
    public void buildSql(String tableName, List<String> fields) {
        buildInsertSql(tableName, fields);
    }

    @Override
    public String buildUpdateSql(String tableName, List<String> fieldNames, Map<String, List<String>> realIndexes, List<String> fullField) {
        return null;
    }

    private void buildInsertSql(String tableName, List<String> fields) {
        StringBuffer sqlBuffer = new StringBuffer();
        //支持Cockroach数据库
        if (isCockroach) {
            //upsert into tableName(col1,col2,...) values (?,?,...)
            sqlBuffer.append("upsert into ".concat(tableName)
                    .concat(" (").concat(StringUtils.join(fields, ",")).concat(") ")
            );
            sqlBuffer.append("values (");
            for (String fieldName : fields) {
                sqlBuffer.append("?,");
            }
            sqlBuffer.deleteCharAt(sqlBuffer.length() - 1);
            sqlBuffer.append(")");

        } else {
            //postgresql
            //insert into test(id_, name_,date_) VALUES(1, 'zs',now()) ON conflict(id_) DO UPDATE SET date_ = excluded.date_,name_=excluded.name_
            sqlBuffer.append("insert into ".concat(tableName)
                    .concat(" (").concat(StringUtils.join(fields, ",")).concat(") ")
            );
            sqlBuffer.append("values (");
            StringBuffer upsertFields = new StringBuffer();
            for (String fieldName : fields) {
                sqlBuffer.append("?,");

                if (isHasKey) {
                    if (fieldName.equals(keyField)) {
                        continue;
                    }
                    upsertFields.append(String.format("%s=excluded.%s,", fieldName, fieldName));
                }
            }
            sqlBuffer.deleteCharAt(sqlBuffer.length() - 1);
            sqlBuffer.append(")");

            if (isHasKey) {
                upsertFields.deleteCharAt(upsertFields.length() - 1);
                sqlBuffer.append(" ON conflict(".concat(keyField).concat(")"));
                sqlBuffer.append(" DO UPDATE SET ");
                sqlBuffer.append(upsertFields);
            }
        }
        this.sql = sqlBuffer.toString();
    }


    @Override
    public String getDriverName() {
        return POSTGRESQL_DRIVER;
    }


}
