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


package com.dtstack.flink.sql.sink.clickhouse;


import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.rdb.RdbSink;
import com.dtstack.flink.sql.sink.rdb.format.RetractJDBCOutputFormat;

import java.util.List;
import java.util.Map;


public class ClickhouseSink extends RdbSink implements IStreamSinkGener<RdbSink> {

    private static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    public ClickhouseSink() {
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
        String sqlTmp = "insert into " +  tableName + " (${fields}) values (${placeholder})";
        String fieldsStr = "";
        String placeholder = "";

        for (String fieldName : fields) {
            fieldsStr += ",`" + fieldName + "`";
            placeholder += ",?";
        }

        fieldsStr = fieldsStr.replaceFirst(",", "");
        placeholder = placeholder.replaceFirst(",", "");

        sqlTmp = sqlTmp.replace("${fields}", fieldsStr).replace("${placeholder}", placeholder);
        this.sql = sqlTmp;
        System.out.println("---insert sql----");
        System.out.println(sql);
    }


    @Override
    public String getDriverName() {
        return CLICKHOUSE_DRIVER;
    }


}
