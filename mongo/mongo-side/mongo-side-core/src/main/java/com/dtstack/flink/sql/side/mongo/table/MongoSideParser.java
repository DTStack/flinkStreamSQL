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

 

package com.dtstack.flink.sql.side.mongo.table;

import com.dtstack.flink.sql.side.mongo.table.MongoSideTableInfo;
import com.dtstack.flink.sql.table.AbsSideTableParser;
import com.dtstack.flink.sql.table.TableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reason:
 * Date: 2018/11/6
 *
 * @author xuqianjin
 */


public class MongoSideParser extends AbsSideTableParser {

    private final static String SIDE_SIGN_KEY = "sideSignKey";

    private final static Pattern SIDE_TABLE_SIGN = Pattern.compile("(?i)^PERIOD\\s+FOR\\s+SYSTEM_TIME$");

    static {
        keyPatternMap.put(SIDE_SIGN_KEY, SIDE_TABLE_SIGN);
        keyHandlerMap.put(SIDE_SIGN_KEY, MongoSideParser::dealSideSign);
    }

    @Override
    public TableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        MongoSideTableInfo MongoTableInfo = new MongoSideTableInfo();
        MongoTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, MongoTableInfo);

        parseCacheProp(MongoTableInfo, props);
        MongoTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(MongoSideTableInfo.PARALLELISM_KEY.toLowerCase())));
        MongoTableInfo.setUrl(MathUtil.getString(props.get(MongoSideTableInfo.URL_KEY.toLowerCase())));
        MongoTableInfo.setTableName(MathUtil.getString(props.get(MongoSideTableInfo.TABLE_NAME_KEY.toLowerCase())));
        MongoTableInfo.setUserName(MathUtil.getString(props.get(MongoSideTableInfo.USER_NAME_KEY.toLowerCase())));
        MongoTableInfo.setPassword(MathUtil.getString(props.get(MongoSideTableInfo.PASSWORD_KEY.toLowerCase())));

        return MongoTableInfo;
    }

    private static void dealSideSign(Matcher matcher, TableInfo tableInfo){
    }
}
