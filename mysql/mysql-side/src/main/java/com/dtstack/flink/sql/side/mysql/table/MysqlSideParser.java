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

 

package com.dtstack.flink.sql.side.mysql.table;

import com.dtstack.flink.sql.table.AbsSideTableParser;
import com.dtstack.flink.sql.table.TableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reason:
 * Date: 2018/7/25
 * Company: www.dtstack.com
 * @author xuchao
 */

public class MysqlSideParser extends AbsSideTableParser {

    private final static String SIDE_SIGN_KEY = "sideSignKey";

    private final static Pattern SIDE_TABLE_SIGN = Pattern.compile("(?i)^PERIOD\\s+FOR\\s+SYSTEM_TIME$");

    static {
        keyPatternMap.put(SIDE_SIGN_KEY, SIDE_TABLE_SIGN);
        keyHandlerMap.put(SIDE_SIGN_KEY, MysqlSideParser::dealSideSign);
    }

    @Override
    public TableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        MysqlSideTableInfo mysqlTableInfo = new MysqlSideTableInfo();
        mysqlTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, mysqlTableInfo);

        parseCacheProp(mysqlTableInfo, props);
        mysqlTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(MysqlSideTableInfo.PARALLELISM_KEY.toLowerCase())));
        mysqlTableInfo.setUrl(MathUtil.getString(props.get(MysqlSideTableInfo.URL_KEY.toLowerCase())));
        mysqlTableInfo.setTableName(MathUtil.getString(props.get(MysqlSideTableInfo.TABLE_NAME_KEY.toLowerCase())));
        mysqlTableInfo.setUserName(MathUtil.getString(props.get(MysqlSideTableInfo.USER_NAME_KEY.toLowerCase())));
        mysqlTableInfo.setPassword(MathUtil.getString(props.get(MysqlSideTableInfo.PASSWORD_KEY.toLowerCase())));

        return mysqlTableInfo;
    }

    private static void dealSideSign(Matcher matcher, TableInfo tableInfo){
    }
}
