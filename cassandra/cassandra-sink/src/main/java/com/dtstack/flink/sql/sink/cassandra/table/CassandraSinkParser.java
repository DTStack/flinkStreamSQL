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


package com.dtstack.flink.sql.sink.cassandra.table;

import com.dtstack.flink.sql.table.AbstractTableParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;

import static com.dtstack.flink.sql.table.AbstractTableInfo.PARALLELISM_KEY;

/**
 * Reason:
 * Date: 2018/11/22
 *
 * @author xuqianjin
 */
public class CassandraSinkParser extends AbstractTableParser {

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

    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        CassandraTableInfo cassandraTableInfo = new CassandraTableInfo();
        cassandraTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, cassandraTableInfo);

        cassandraTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(PARALLELISM_KEY.toLowerCase())));
        cassandraTableInfo.setAddress(MathUtil.getString(props.get(ADDRESS_KEY.toLowerCase())));
        cassandraTableInfo.setTableName(MathUtil.getString(props.get(TABLE_NAME_KEY.toLowerCase())));
        cassandraTableInfo.setDatabase(MathUtil.getString(props.get(DATABASE_KEY.toLowerCase())));
        cassandraTableInfo.setUserName(MathUtil.getString(props.get(USER_NAME_KEY.toLowerCase())));
        cassandraTableInfo.setPassword(MathUtil.getString(props.get(PASSWORD_KEY.toLowerCase())));
        cassandraTableInfo.setMaxRequestsPerConnection(MathUtil.getIntegerVal(props.get(MAX_REQUEST_PER_CONNECTION_KEY.toLowerCase())));
        cassandraTableInfo.setCoreConnectionsPerHost(MathUtil.getIntegerVal(props.get(CORE_CONNECTIONS_PER_HOST_KEY.toLowerCase())));
        cassandraTableInfo.setMaxConnectionsPerHost(MathUtil.getIntegerVal(props.get(MAX_CONNECTIONS_PER_HOST_KEY.toLowerCase())));
        cassandraTableInfo.setMaxQueueSize(MathUtil.getIntegerVal(props.get(MAX_QUEUE_SIZE_KEY.toLowerCase())));
        cassandraTableInfo.setReadTimeoutMillis(MathUtil.getIntegerVal(props.get(READ_TIMEOUT_MILLIS_KEY.toLowerCase())));
        cassandraTableInfo.setConnectTimeoutMillis(MathUtil.getIntegerVal(props.get(CONNECT_TIMEOUT_MILLIS_KEY.toLowerCase())));
        cassandraTableInfo.setPoolTimeoutMillis(MathUtil.getIntegerVal(props.get(POOL_TIMEOUT_MILLIS_KEY.toLowerCase())));

        return cassandraTableInfo;
    }
}
