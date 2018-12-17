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

package com.dtstack.flink.sql.side.wredis.table;

import com.dtstack.flink.sql.table.AbsSideTableParser;
import com.dtstack.flink.sql.table.TableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WredisSideParser extends AbsSideTableParser {

    private final static String SIDE_SIGN_KEY = "sideSignKey";

    private final static Pattern SIDE_TABLE_SIGN = Pattern.compile("(?i)^PERIOD\\s+FOR\\s+SYSTEM_TIME$");

    static {
        keyPatternMap.put(SIDE_SIGN_KEY, SIDE_TABLE_SIGN);
        keyHandlerMap.put(SIDE_SIGN_KEY, WredisSideParser::dealSideSign);
    }

    @Override
    public TableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        WredisSideTableInfo redisSideTableInfo = new WredisSideTableInfo();
        redisSideTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, redisSideTableInfo);
        parseCacheProp(redisSideTableInfo, props);
        redisSideTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(WredisSideTableInfo.PARALLELISM_KEY.toLowerCase())));
        redisSideTableInfo.setUrl(MathUtil.getString(props.get(WredisSideTableInfo.URL_KEY)));
        redisSideTableInfo.setPassword(MathUtil.getString(props.get(WredisSideTableInfo.PASSWORD_KEY)));
        redisSideTableInfo.setKey(MathUtil.getString(props.get(WredisSideTableInfo.KEY_KEY)));
        redisSideTableInfo.setTableName(MathUtil.getString(props.get(WredisSideTableInfo.TABLENAME_KEY)));
        if (props.get(WredisSideTableInfo.TIMEOUT) != null){
            redisSideTableInfo.setTimeout(Integer.parseInt(MathUtil.getString(props.get(WredisSideTableInfo.TIMEOUT))));
        }
        return redisSideTableInfo;
    }

    private static void dealSideSign(Matcher matcher, TableInfo tableInfo){
    }
}
