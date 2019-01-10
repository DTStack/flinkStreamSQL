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

 

package com.dtstack.flink.sql.side.wtable.table;

import com.dtstack.flink.sql.table.AbsSideTableParser;
import com.dtstack.flink.sql.table.TableInfo;
import com.dtstack.flink.sql.util.ClassUtil;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.dtstack.flink.sql.table.TableInfo.PARALLELISM_KEY;

/**
 * hbase field information must include the definition of an alias -> sql which does not allow ":"
 * Date: 2018/8/21
 * Company: www.dtstack.com
 * @author xuchao
 */

public class WtableSideParser extends AbsSideTableParser {
    private final static String SIDE_SIGN_KEY = "sideSignKey";

    private final static Pattern SIDE_TABLE_SIGN = Pattern.compile("(?i)^PERIOD\\s+FOR\\s+SYSTEM_TIME$");

    public static final String NAME_CENTER = "nameCenter";

    public static final String BID = "bid";

    public static final String PASSWORD = "password";

    public static final String TABLE_ID_KEY = "tableid";

    static {
        keyPatternMap.put(SIDE_SIGN_KEY, SIDE_TABLE_SIGN);
        keyHandlerMap.put(SIDE_SIGN_KEY, WtableSideParser::dealSideSign);
    }


    @Override
    public TableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        WtableSideTableInfo wtableTableInfo = new WtableSideTableInfo();
        wtableTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, wtableTableInfo);
        parseCacheProp(wtableTableInfo, props);
        wtableTableInfo.setNameCenter((String) props.get(NAME_CENTER.toLowerCase()));
        wtableTableInfo.setBid((String)props.get(BID.toLowerCase()));
        wtableTableInfo.setPassword((String)props.get(PASSWORD.toLowerCase()));
        wtableTableInfo.setTableId(MathUtil.getIntegerVal(props.get(TABLE_ID_KEY.toLowerCase())));
        wtableTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(PARALLELISM_KEY.toLowerCase())));
        return wtableTableInfo;
    }

    private static void dealSideSign(Matcher matcher, TableInfo tableInfo){
        //FIXME 暂时不适用该标识--仅仅只是作为一个标识适用
    }
}
