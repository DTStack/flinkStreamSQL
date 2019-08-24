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


package com.dtstack.flink.sql.sink.postgresql.table;

import com.dtstack.flink.sql.sink.rdb.table.RdbSinkParser;
import com.dtstack.flink.sql.sink.rdb.table.RdbTableInfo;
import com.dtstack.flink.sql.table.TableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;

/**
 * Date: 2019-08-22
 * Company: mmg
 *
 * @author tcm
 */

public class PostgresqlSinkParser extends RdbSinkParser {
    private static final String CURR_TYPE = "postgresql";
    public static final String IS_COCKROACH = "isCockroach";
    public static final String KEY_FIELD = "keyField";

    @Override
    public TableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
       /* TableInfo tableInfo = (TableInfo) super.getTableInfo(tableName, fieldsInfo, props);
        tableInfo.setType(CURR_TYPE);
        return tableInfo;*/
        PgTableInfo pgTableInfo = new PgTableInfo();
        pgTableInfo.setType(CURR_TYPE);
        String isCC = (String) props.get(IS_COCKROACH.toLowerCase());
        pgTableInfo.setCockroach(isCC.equals("true") ? true : false);
        pgTableInfo.setKeyField((String) props.get(KEY_FIELD.toLowerCase()));

        pgTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, pgTableInfo);

        pgTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(RdbTableInfo.PARALLELISM_KEY.toLowerCase())));
        pgTableInfo.setUrl(MathUtil.getString(props.get(RdbTableInfo.URL_KEY.toLowerCase())));
        pgTableInfo.setTableName(MathUtil.getString(props.get(RdbTableInfo.TABLE_NAME_KEY.toLowerCase())));
        pgTableInfo.setUserName(MathUtil.getString(props.get(RdbTableInfo.USER_NAME_KEY.toLowerCase())));
        pgTableInfo.setPassword(MathUtil.getString(props.get(RdbTableInfo.PASSWORD_KEY.toLowerCase())));
        pgTableInfo.setBatchSize(MathUtil.getIntegerVal(props.get(RdbTableInfo.BATCH_SIZE_KEY.toLowerCase())));
        pgTableInfo.setBatchWaitInterval(MathUtil.getLongVal(props.get(RdbTableInfo.BATCH_WAIT_INTERVAL_KEY.toLowerCase())));
        pgTableInfo.setBufferSize(MathUtil.getString(props.get(RdbTableInfo.BUFFER_SIZE_KEY.toLowerCase())));
        pgTableInfo.setFlushIntervalMs(MathUtil.getString(props.get(RdbTableInfo.FLUSH_INTERVALMS_KEY.toLowerCase())));

        pgTableInfo.check();
        return pgTableInfo;
    }
}
