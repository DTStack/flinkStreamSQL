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

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.cassandra.CassandraAllSideInfo;
import com.dtstack.flink.sql.side.table.BaseTableFunction;
import com.dtstack.flink.sql.util.SwitchUtil;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author: chuixue
 * @create: 2020-11-19 14:21
 * @description:
 **/
public class CassandraTableFunction extends BaseTableFunction {
    private static final long serialVersionUID = 54015343561288219L;

    private static final Logger LOG = LoggerFactory.getLogger(CassandraTableFunction.class);
    private transient Cluster cluster;
    private transient Session session = null;

    public CassandraTableFunction(AbstractSideTableInfo sideTableInfo, String[] lookupKeys) {
        super(new CassandraAllSideInfo(sideTableInfo, lookupKeys));
    }

    private Session getConn(CassandraSideTableInfo tableInfo) {
        try {
            if (session == null) {
                QueryOptions queryOptions = new QueryOptions();
                //The default consistency level for queries: ConsistencyLevel.TWO.
                queryOptions.setConsistencyLevel(ConsistencyLevel.QUORUM);
                Integer maxRequestsPerConnection = tableInfo.getMaxRequestsPerConnection() == null ? 1 : tableInfo.getMaxRequestsPerConnection();
                Integer coreConnectionsPerHost = tableInfo.getCoreConnectionsPerHost() == null ? 8 : tableInfo.getCoreConnectionsPerHost();
                Integer maxConnectionsPerHost = tableInfo.getMaxConnectionsPerHost() == null ? 32768 : tableInfo.getMaxConnectionsPerHost();
                Integer maxQueueSize = tableInfo.getMaxQueueSize() == null ? 100000 : tableInfo.getMaxQueueSize();
                Integer readTimeoutMillis = tableInfo.getReadTimeoutMillis() == null ? 60000 : tableInfo.getReadTimeoutMillis();
                Integer connectTimeoutMillis = tableInfo.getConnectTimeoutMillis() == null ? 60000 : tableInfo.getConnectTimeoutMillis();
                Integer poolTimeoutMillis = tableInfo.getPoolTimeoutMillis() == null ? 60000 : tableInfo.getPoolTimeoutMillis();
                Integer cassandraPort = 0;
                String address = tableInfo.getAddress();
                String userName = tableInfo.getUserName();
                String password = tableInfo.getPassword();
                String database = tableInfo.getDatabase();

                ArrayList serversList = new ArrayList();
                //Read timeout or connection timeout Settings
                SocketOptions so = new SocketOptions()
                        .setReadTimeoutMillis(readTimeoutMillis)
                        .setConnectTimeoutMillis(connectTimeoutMillis);

                //The cluster USES hostdistance.local in the same machine room
                //Hostdistance. REMOTE is used for different machine rooms
                //Ignore use HostDistance. IGNORED
                PoolingOptions poolingOptions = new PoolingOptions()
                        //Each connection allows a maximum of 64 concurrent requests
                        .setMaxRequestsPerConnection(HostDistance.LOCAL, maxRequestsPerConnection)
                        //Have at least two connections to each machine in the cluster
                        .setCoreConnectionsPerHost(HostDistance.LOCAL, coreConnectionsPerHost)
                        //There are up to eight connections to each machine in the cluster
                        .setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnectionsPerHost)
                        .setMaxQueueSize(maxQueueSize)
                        .setPoolTimeoutMillis(poolTimeoutMillis);
                //重试策略
                RetryPolicy retryPolicy = DowngradingConsistencyRetryPolicy.INSTANCE;

                for (String server : StringUtils.split(address, ",")) {
                    cassandraPort = Integer.parseInt(StringUtils.split(server, ":")[1]);
                    serversList.add(InetAddress.getByName(StringUtils.split(server, ":")[0]));
                }

                if (userName == null || userName.isEmpty() || password == null || password.isEmpty()) {
                    cluster = Cluster.builder().addContactPoints(serversList).withRetryPolicy(retryPolicy)
                            .withPort(cassandraPort)
                            .withPoolingOptions(poolingOptions).withSocketOptions(so)
                            .withQueryOptions(queryOptions).build();
                } else {
                    cluster = Cluster.builder().addContactPoints(serversList).withRetryPolicy(retryPolicy)
                            .withPort(cassandraPort)
                            .withPoolingOptions(poolingOptions).withSocketOptions(so)
                            .withCredentials(userName, password)
                            .withQueryOptions(queryOptions).build();
                }
                // 建立连接 连接已存在的键空间
                session = cluster.connect(database);
                LOG.info("connect cassandra is successed!");
            }
        } catch (Exception e) {
            LOG.error("connect cassandra is error:" + e.getMessage());
        }
        return session;
    }

    @Override
    protected void loadData(Object cacheRef) {
        Map<String, List<Map<String, Object>>> tmpCache = (Map<String, List<Map<String, Object>>>) cacheRef;
        CassandraSideTableInfo tableInfo = (CassandraSideTableInfo) sideTableInfo;
        Session session = null;

        try {
            for (int i = 0; i < CONN_RETRY_NUM; i++) {
                try {
                    session = getConn(tableInfo);
                    break;
                } catch (Exception e) {
                    if (i == CONN_RETRY_NUM - 1) {
                        throw new RuntimeException("", e);
                    }
                    try {
                        String connInfo = "address:" + tableInfo.getAddress() + ";userName:" + tableInfo.getUserName()
                                + ",pwd:" + tableInfo.getPassword();
                        LOG.warn("get conn fail, wait for 5 sec and try again, connInfo:" + connInfo);
                        Thread.sleep(LOAD_DATA_ERROR_SLEEP_TIME);
                    } catch (InterruptedException e1) {
                        LOG.error("", e1);
                    }
                }

            }

            //load data from table
            String sql = sideInfo.getSqlCondition() + " limit " + getFetchSize();
            ResultSet resultSet = session.execute(sql);
            String[] sideFieldNames = physicalFields.values().stream().toArray(String[]::new);
            String[] sideFieldTypes = tableInfo.getFieldTypes();
            for (com.datastax.driver.core.Row row : resultSet) {
                Map<String, Object> oneRow = Maps.newHashMap();
                // 防止一条数据有问题，后面数据无法加载
                try {
                    for (int i = 0; i < sideFieldNames.length; i++) {
                        Object object = row.getObject(sideFieldNames[i].trim());
                        object = SwitchUtil.getTarget(object, sideFieldTypes[i]);
                        oneRow.put(sideFieldNames[i].trim(), object);
                    }
                    buildCache(oneRow, tmpCache);
                } catch (Exception e) {
                    LOG.error("", e);
                }
            }
        } catch (Exception e) {
            LOG.error("", e);
        } finally {
            try {
                if (session != null) {
                    session.close();
                }
            } catch (Exception e) {
                LOG.error("Error while closing session.", e);
            }
            try {
                if (cluster != null) {
                    cluster.close();
                }
            } catch (Exception e) {
                LOG.error("Error while closing cluster.", e);
            }
        }
    }
}
