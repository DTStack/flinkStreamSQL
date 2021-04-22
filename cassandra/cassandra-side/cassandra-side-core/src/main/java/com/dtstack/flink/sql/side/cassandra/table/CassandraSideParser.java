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


package com.dtstack.flink.sql.side.cassandra.table;

import com.dtstack.flink.sql.table.AbstractSideTableParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.sql.Timestamp;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.dtstack.flink.sql.table.AbstractTableInfo.PARALLELISM_KEY;

/**
 * Reason:
 * Date: 2018/11/22
 *
 * @author xuqianjin
 */
public class CassandraSideParser extends AbstractSideTableParser {

    private final static String SIDE_SIGN_KEY = "sideSignKey";

    private final static Pattern SIDE_TABLE_SIGN = Pattern.compile("(?i)^PERIOD\\s+FOR\\s+SYSTEM_TIME$");

    public static final String ADDRESS_KEY = "address";

    public static final String TABLE_NAME_KEY = "tableName";

    public static final String USER_NAME_KEY = "userName";

    public static final String PASSWORD_KEY = "password";

    public static final String DATABASE_KEY = "database";

    public static final String MAX_REQUEST_PER_CONNECTION_KEY = "maxRequestsPerConnection";

    public static final String CORE_CONNECTIONS_PER_HOST_KEY = "coreConnectionsPerHost";

    public static final String MAX_CONNECTIONS_PER_HOST_KEY = "maxConnectionsPerHost";

    public static final String MAX_QUEUE_SIZE_KEY = "maxQueueSize";

    public static final String READ_TIMEOUT_MILLIS_KEY = "readTimeoutMillis";

    public static final String CONNECT_TIMEOUT_MILLIS_KEY = "connectTimeoutMillis";

    public static final String POOL_TIMEOUT_MILLIS_KEY = "poolTimeoutMillis";

    public CassandraSideParser() {
        addParserHandler(SIDE_SIGN_KEY, SIDE_TABLE_SIGN, this::dealSideSign);
    }

    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        com.dtstack.flink.sql.side.cassandra.table.CassandraSideTableInfo cassandraSideTableInfo = new com.dtstack.flink.sql.side.cassandra.table.CassandraSideTableInfo();
        cassandraSideTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, cassandraSideTableInfo);
        parseCacheProp(cassandraSideTableInfo, props);

        cassandraSideTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(PARALLELISM_KEY.toLowerCase())));
        cassandraSideTableInfo.setAddress(MathUtil.getString(props.get(ADDRESS_KEY.toLowerCase())));
        cassandraSideTableInfo.setTableName(MathUtil.getString(props.get(TABLE_NAME_KEY.toLowerCase())));
        cassandraSideTableInfo.setDatabase(MathUtil.getString(props.get(DATABASE_KEY.toLowerCase())));
        cassandraSideTableInfo.setUserName(MathUtil.getString(props.get(USER_NAME_KEY.toLowerCase())));
        cassandraSideTableInfo.setPassword(MathUtil.getString(props.get(PASSWORD_KEY.toLowerCase())));
        cassandraSideTableInfo.setMaxRequestsPerConnection(MathUtil.getIntegerVal(props.get(MAX_REQUEST_PER_CONNECTION_KEY.toLowerCase())));
        cassandraSideTableInfo.setCoreConnectionsPerHost(MathUtil.getIntegerVal(props.get(CORE_CONNECTIONS_PER_HOST_KEY.toLowerCase())));
        cassandraSideTableInfo.setMaxConnectionsPerHost(MathUtil.getIntegerVal(props.get(MAX_CONNECTIONS_PER_HOST_KEY.toLowerCase())));
        cassandraSideTableInfo.setMaxQueueSize(MathUtil.getIntegerVal(props.get(MAX_QUEUE_SIZE_KEY.toLowerCase())));
        cassandraSideTableInfo.setReadTimeoutMillis(MathUtil.getIntegerVal(props.get(READ_TIMEOUT_MILLIS_KEY.toLowerCase())));
        cassandraSideTableInfo.setConnectTimeoutMillis(MathUtil.getIntegerVal(props.get(CONNECT_TIMEOUT_MILLIS_KEY.toLowerCase())));
        cassandraSideTableInfo.setPoolTimeoutMillis(MathUtil.getIntegerVal(props.get(POOL_TIMEOUT_MILLIS_KEY.toLowerCase())));

        if (MathUtil.getLongVal(props.get(cassandraSideTableInfo.ERROR_LIMIT.toLowerCase())) != null) {
            cassandraSideTableInfo.setErrorLimit(MathUtil.getLongVal(props.get(cassandraSideTableInfo.ERROR_LIMIT.toLowerCase())));
        }

        return cassandraSideTableInfo;
    }

    private void dealSideSign(Matcher matcher, AbstractTableInfo tableInfo) {
    }

    @Override
    public Class dbTypeConvertToJavaType(String fieldType) {
        switch (fieldType.toLowerCase()) {
            case "bigint":
                return Long.class;
            case "int":
            case "counter":
                return Integer.class;

            case "text":
            case "inet":
            case "varchar":
            case "ascii":
            case "timeuuid":
                return String.class;

            case "decimal":
            case "float":
                return Float.class;
            case "double":
                return Double.class;
            case "timestamp":
                return Timestamp.class;
            default:
                break;
        }

        throw new RuntimeException("不支持 " + fieldType + " 类型");

    }
}
