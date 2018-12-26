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

package com.dtstack.flink.sql.sink.wredis.table;

import com.dtstack.flink.sql.table.AbsTableParser;
import com.dtstack.flink.sql.table.TableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;

import static com.dtstack.flink.sql.table.TableInfo.PARALLELISM_KEY;

public class WredisSinkParser extends AbsTableParser {
    @Override
    public TableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        WredisTableInfo redisTableInfo = new WredisTableInfo();
        redisTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, redisTableInfo);
        redisTableInfo.setUrl(MathUtil.getString(props.get(WredisTableInfo.URL_KEY)));
        redisTableInfo.setKey(MathUtil.getString(props.get(WredisTableInfo.KEY_KEY)));
        redisTableInfo.setPassword(MathUtil.getString(props.get(WredisTableInfo.PASSWORD_KEY)));
        redisTableInfo.setTableName(MathUtil.getString(props.get(WredisTableInfo.TABLENAME_KEY)));
        redisTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(PARALLELISM_KEY.toLowerCase())));
        if (props.get(WredisTableInfo.TIMEOUT) != null){
            redisTableInfo.setTimeout(Integer.parseInt(MathUtil.getString(props.get(WredisTableInfo.TIMEOUT))));
        }
        return redisTableInfo;
    }
}
