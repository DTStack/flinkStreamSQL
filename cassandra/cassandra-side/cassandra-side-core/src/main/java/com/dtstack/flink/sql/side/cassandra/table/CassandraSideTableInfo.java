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

import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.google.common.base.Preconditions;

/**
 * Reason:
 * Date: 2018/11/22
 *
 * @author xuqianjin
 */
public class CassandraSideTableInfo extends AbstractSideTableInfo {

    private static final long serialVersionUID = -5556431094535478915L;

    private static final String CURR_TYPE = "cassandra";

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

    private String address;
    private String tableName;
    private String userName;
    private String password;
    private String database;
    private Integer maxRequestsPerConnection;
    private Integer coreConnectionsPerHost;
    private Integer maxConnectionsPerHost;
    private Integer maxQueueSize;
    private Integer readTimeoutMillis;
    private Integer connectTimeoutMillis;
    private Integer poolTimeoutMillis;

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Integer getMaxRequestsPerConnection() {
        return maxRequestsPerConnection;
    }

    public void setMaxRequestsPerConnection(Integer maxRequestsPerConnection) {
        this.maxRequestsPerConnection = maxRequestsPerConnection;
    }

    public Integer getCoreConnectionsPerHost() {
        return coreConnectionsPerHost;
    }

    public void setCoreConnectionsPerHost(Integer coreConnectionsPerHost) {
        this.coreConnectionsPerHost = coreConnectionsPerHost;
    }

    public Integer getMaxConnectionsPerHost() {
        return maxConnectionsPerHost;
    }

    public void setMaxConnectionsPerHost(Integer maxConnectionsPerHost) {
        this.maxConnectionsPerHost = maxConnectionsPerHost;
    }

    public Integer getMaxQueueSize() {
        return maxQueueSize;
    }

    public void setMaxQueueSize(Integer maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    public Integer getReadTimeoutMillis() {
        return readTimeoutMillis;
    }

    public void setReadTimeoutMillis(Integer readTimeoutMillis) {
        this.readTimeoutMillis = readTimeoutMillis;
    }

    public Integer getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public void setConnectTimeoutMillis(Integer connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    public Integer getPoolTimeoutMillis() {
        return poolTimeoutMillis;
    }

    public void setPoolTimeoutMillis(Integer poolTimeoutMillis) {
        this.poolTimeoutMillis = poolTimeoutMillis;
    }

    public CassandraSideTableInfo() {
        setType(CURR_TYPE);
    }

    @Override
    public boolean check() {
        Preconditions.checkNotNull(address, "Cassandra field of ADDRESS is required");
        Preconditions.checkNotNull(database, "Cassandra field of database is required");
        Preconditions.checkNotNull(tableName, "Cassandra field of tableName is required");
        return true;
    }
}
