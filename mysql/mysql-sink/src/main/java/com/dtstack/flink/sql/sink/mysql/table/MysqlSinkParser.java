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

 

package com.dtstack.flink.sql.sink.mysql.table;

import com.dtstack.flink.sql.table.AbsTableParser;
import com.dtstack.flink.sql.table.TableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;

/**
 * Reason:
 * Date: 2018/7/4
 * Company: www.dtstack.com
 * @author xuchao
 */

public class MysqlSinkParser extends AbsTableParser {

    @Override
    public TableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        MysqlTableInfo mysqlTableInfo = new MysqlTableInfo();
        mysqlTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, mysqlTableInfo);

        mysqlTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(MysqlTableInfo.PARALLELISM_KEY.toLowerCase())));
        mysqlTableInfo.setUrl(MathUtil.getString(props.get(MysqlTableInfo.URL_KEY.toLowerCase())));
        mysqlTableInfo.setTableName(MathUtil.getString(props.get(MysqlTableInfo.TABLE_NAME_KEY.toLowerCase())));
        mysqlTableInfo.setUserName(MathUtil.getString(props.get(MysqlTableInfo.USER_NAME_KEY.toLowerCase())));
        mysqlTableInfo.setPassword(MathUtil.getString(props.get(MysqlTableInfo.PASSWORD_KEY.toLowerCase())));
        mysqlTableInfo.setBatchSize(MathUtil.getIntegerVal(props.get(MysqlTableInfo.BATCH_SIZE_KEY.toLowerCase())));
        mysqlTableInfo.setBufferSize(MathUtil.getString(props.get(MysqlTableInfo.BUFFER_SIZE_KEY.toLowerCase())));
        mysqlTableInfo.setFlushIntervalMs(MathUtil.getString(props.get(MysqlTableInfo.FLUSH_INTERVALMS_KEY.toLowerCase())));

        return mysqlTableInfo;
    }
}
