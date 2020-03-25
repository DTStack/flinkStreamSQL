/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flink.sql.sink.rdb.table;

import com.dtstack.flink.sql.table.AbstractTableParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;

/**
 * Reason:
 * Date: 2018/11/27
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class RdbSinkParser extends AbstractTableParser {
    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        RdbTableInfo rdbTableInfo = new RdbTableInfo();
        rdbTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, rdbTableInfo);

        rdbTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(RdbTableInfo.PARALLELISM_KEY.toLowerCase())));
        rdbTableInfo.setUrl(MathUtil.getString(props.get(RdbTableInfo.URL_KEY.toLowerCase())));
        rdbTableInfo.setTableName(MathUtil.getString(props.get(RdbTableInfo.TABLE_NAME_KEY.toLowerCase())));
        rdbTableInfo.setUserName(MathUtil.getString(props.get(RdbTableInfo.USER_NAME_KEY.toLowerCase())));
        rdbTableInfo.setPassword(MathUtil.getString(props.get(RdbTableInfo.PASSWORD_KEY.toLowerCase())));
        rdbTableInfo.setBatchSize(MathUtil.getIntegerVal(props.get(RdbTableInfo.BATCH_SIZE_KEY.toLowerCase())));
        rdbTableInfo.setBatchWaitInterval(MathUtil.getLongVal(props.get(RdbTableInfo.BATCH_WAIT_INTERVAL_KEY.toLowerCase())));
        rdbTableInfo.setBufferSize(MathUtil.getString(props.get(RdbTableInfo.BUFFER_SIZE_KEY.toLowerCase())));
        rdbTableInfo.setFlushIntervalMs(MathUtil.getString(props.get(RdbTableInfo.FLUSH_INTERVALMS_KEY.toLowerCase())));
        rdbTableInfo.setSchema(MathUtil.getString(props.get(RdbTableInfo.SCHEMA_KEY.toLowerCase())));
        rdbTableInfo.setUpdateMode(MathUtil.getString(props.get(RdbTableInfo.UPDATE_KEY.toLowerCase())));
        rdbTableInfo.setAllReplace(MathUtil.getBoolean(props.get(RdbTableInfo.ALLREPLACE_KEY.toLowerCase()), false));

        rdbTableInfo.check();
        return rdbTableInfo;
    }
}
