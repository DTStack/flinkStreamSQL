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

package com.dtstack.flink.sql.side.redis.table;

import com.dtstack.flink.sql.table.AbsSideTableParser;
import com.dtstack.flink.sql.table.TableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RedisSideParser extends AbsSideTableParser {

    private final static String SIDE_SIGN_KEY = "sideSignKey";

    private final static Pattern SIDE_TABLE_SIGN = Pattern.compile("(?i)^PERIOD\\s+FOR\\s+SYSTEM_TIME$");

    static {
        keyPatternMap.put(SIDE_SIGN_KEY, SIDE_TABLE_SIGN);
        keyHandlerMap.put(SIDE_SIGN_KEY, RedisSideParser::dealSideSign);
    }

    @Override
    public TableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        RedisSideTableInfo redisSideTableInfo = new RedisSideTableInfo();
        redisSideTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, redisSideTableInfo);
        parseCacheProp(redisSideTableInfo, props);
        redisSideTableInfo.setUrl(MathUtil.getString(props.get(RedisSideTableInfo.URL_KEY)));
        redisSideTableInfo.setPassword(MathUtil.getString(props.get(RedisSideTableInfo.PASSWORD_KEY)));
        redisSideTableInfo.setDatabase(MathUtil.getString(props.get(RedisSideTableInfo.DATABASE_KEY)));
        redisSideTableInfo.setTableName(MathUtil.getString(props.get(RedisSideTableInfo.TABLE_KEY)));
        if (props.get(RedisSideTableInfo.TIMEOUT) != null){
         redisSideTableInfo.setTimeout(MathUtil.getIntegerVal(props.get(RedisSideTableInfo.TIMEOUT)));
        }
        redisSideTableInfo.setMaxTotal(MathUtil.getString(props.get(RedisSideTableInfo.MAXTOTAL)));
        redisSideTableInfo.setMaxIdle(MathUtil.getString(props.get(RedisSideTableInfo.MAXIDLE)));
        redisSideTableInfo.setMinIdle(MathUtil.getString(props.get(RedisSideTableInfo.MINIDLE)));
        redisSideTableInfo.setMasterName(MathUtil.getString(props.get(RedisSideTableInfo.MASTER_NAME)));
        redisSideTableInfo.setRedisType(MathUtil.getIntegerVal(RedisSideTableInfo.REDIS_TYPE));
        return redisSideTableInfo;
    }

    private static void dealSideSign(Matcher matcher, TableInfo tableInfo){
    }
}
