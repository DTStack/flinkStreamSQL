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


package com.dtstack.flink.sql.sink.clickhouse.table;

import com.dtstack.flink.sql.sink.rdb.table.RdbSinkParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import ru.yandex.clickhouse.domain.ClickHouseDataType;

import java.util.Map;


public class ClickhouseSinkParser extends RdbSinkParser {
    private static final String CURR_TYPE = "clickhouse";

    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        AbstractTableInfo clickhouseTableInfo = super.getTableInfo(tableName, fieldsInfo, props);
        clickhouseTableInfo.setType(CURR_TYPE);
        return clickhouseTableInfo;
    }

    @Override
    public Class dbTypeConvertToJavaType(String fieldType) {
        return ClickHouseDataType.fromTypeString(fieldType).getJavaClass();
    }

}