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
import com.dtstack.flink.sql.table.TableInfo;
import com.dtstack.flink.sql.util.MathUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * Date: 2019-08-22
 * Company: mmg
 *
 * @author tcm
 */

public class PostgresqlSinkParser extends RdbSinkParser {
    private static final String CURR_TYPE = "postgresql";

    @Override
    public TableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        PostgresqlTableInfo pgTableInfo = new PostgresqlTableInfo();
        pgTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, pgTableInfo);

        pgTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(PostgresqlTableInfo.PARALLELISM_KEY.toLowerCase())));
        pgTableInfo.setUrl(MathUtil.getString(props.get(PostgresqlTableInfo.URL_KEY.toLowerCase())));
        pgTableInfo.setTableName(MathUtil.getString(props.get(PostgresqlTableInfo.TABLE_NAME_KEY.toLowerCase())));
        pgTableInfo.setUserName(MathUtil.getString(props.get(PostgresqlTableInfo.USER_NAME_KEY.toLowerCase())));
        pgTableInfo.setPassword(MathUtil.getString(props.get(PostgresqlTableInfo.PASSWORD_KEY.toLowerCase())));
        pgTableInfo.setBatchSize(MathUtil.getIntegerVal(props.get(PostgresqlTableInfo.BATCH_SIZE_KEY.toLowerCase())));
        pgTableInfo.setBatchWaitInterval(MathUtil.getLongVal(props.get(PostgresqlTableInfo.BATCH_WAIT_INTERVAL_KEY.toLowerCase())));
        pgTableInfo.setBufferSize(MathUtil.getString(props.get(PostgresqlTableInfo.BUFFER_SIZE_KEY.toLowerCase())));
        pgTableInfo.setFlushIntervalMs(MathUtil.getString(props.get(PostgresqlTableInfo.FLUSH_INTERVALMS_KEY.toLowerCase())));

        pgTableInfo.setKeyField(MathUtil.getString(props.get(PostgresqlTableInfo.TABLE_KEY_FIELD.toLowerCase())));

        String isUpsertStr = (String) props.get(PostgresqlTableInfo.TABLE_IS_UPSERT.toLowerCase());
        pgTableInfo.setUpsert(!StringUtils.isEmpty(isUpsertStr) && isUpsertStr.equals("true") ? true : false);

        pgTableInfo.check();
        return pgTableInfo;
    }
}
