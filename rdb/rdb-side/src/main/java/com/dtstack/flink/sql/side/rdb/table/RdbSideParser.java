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


package com.dtstack.flink.sql.side.rdb.table;

import com.dtstack.flink.sql.table.AbstractSideTableParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;

import static com.dtstack.flink.sql.side.rdb.table.RdbSideTableInfo.DRIVER_NAME;

/**
 * Reason:
 * Date: 2018/11/26
 * Company: www.dtstack.com
 *
 * @author maqi
 */

public class RdbSideParser extends AbstractSideTableParser {

    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        RdbSideTableInfo rdbTableInfo = new RdbSideTableInfo();
        rdbTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, rdbTableInfo);

        parseCacheProp(rdbTableInfo, props);
        rdbTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(RdbSideTableInfo.PARALLELISM_KEY.toLowerCase())));
        rdbTableInfo.setUrl(MathUtil.getString(props.get(RdbSideTableInfo.URL_KEY.toLowerCase())));
        rdbTableInfo.setTableName(MathUtil.getString(props.get(RdbSideTableInfo.TABLE_NAME_KEY.toLowerCase())));
        rdbTableInfo.setUserName(MathUtil.getString(props.get(RdbSideTableInfo.USER_NAME_KEY.toLowerCase())));
        rdbTableInfo.setPassword(MathUtil.getString(props.get(RdbSideTableInfo.PASSWORD_KEY.toLowerCase())));
        rdbTableInfo.setSchema(MathUtil.getString(props.get(RdbSideTableInfo.SCHEMA_KEY.toLowerCase())));
        rdbTableInfo.setDriverName(MathUtil.getString(props.get(RdbSideTableInfo.DRIVER_NAME)));
        rdbTableInfo.setFastCheck(MathUtil.getBoolean(props.getOrDefault(RdbSideTableInfo.FAST_CHECK.toLowerCase(), true)));

        rdbTableInfo.check();
        return rdbTableInfo;
    }
}
