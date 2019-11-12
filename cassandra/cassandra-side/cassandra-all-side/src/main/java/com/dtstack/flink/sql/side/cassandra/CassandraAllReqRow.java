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

package com.dtstack.flink.sql.side.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.dtstack.flink.sql.side.AllReqRow;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.SideTableInfo;
import com.dtstack.flink.sql.side.cassandra.table.CassandraSideTableInfo;
import org.apache.calcite.sql.JoinType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Reason:
 * Date: 2018/11/22
 *
 * @author xuqianjin
 */
public class CassandraAllReqRow extends AllReqRow {

    private static final long serialVersionUID = 54015343561288219L;

    private static final Logger LOG = LoggerFactory.getLogger(CassandraAllReqRow.class);

    private static final String cassandra_DRIVER = "com.cassandra.jdbc.Driver";

    private static final int CONN_RETRY_NUM = 3;

    private static final int FETCH_SIZE = 1000;

    private transient Cluster cluster;
    private transient Session session = null;

    private AtomicReference<Map<String, List<Map<String, Object>>>> cacheRef = new AtomicReference<>();

    public CassandraAllReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(new com.dtstack.flink.sql.side.cassandra.CassandraAllSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
    }

    @Override
    public Row fillData(Row input, Object sideInput) {
        Map<String, Object> cacheInfo = (Map<String, Object>) sideInput;
        Row row = new Row(sideInfo.getOutFieldInfoList().size());
        for (Map.Entry<Integer, Integer> entry : sideInfo.getInFieldIndex().entrySet()) {
            Object obj = input.getField(entry.getValue());
            boolean isTimeIndicatorTypeInfo = TimeIndicatorTypeInfo.class.isAssignableFrom(sideInfo.getRowTypeInfo().getTypeAt(entry.getValue()).getClass());

            //Type information for indicating event or processing time. However, it behaves like a regular SQL timestamp but is serialized as Long.
            if (obj instanceof Timestamp && isTimeIndicatorTypeInfo) {
                obj = ((Timestamp) obj).getTime();
            }
            row.setField(entry.getKey(), obj);
        }

        for (Map.Entry<Integer, String> entry : sideInfo.getSideFieldNameIndex().entrySet()) {
            if (cacheInfo == null) {
                row.setField(entry.getKey(), null);
            } else {
                row.setField(entry.getKey(), cacheInfo.get(entry.getValue()));
            }
        }

        return row;
    }

    @Override
    protected void initCache() throws SQLException {
        Map<String, List<Map<String, Object>>> newCache = Maps.newConcurrentMap();
        cacheRef.set(newCache);
        loadData(newCache);
    }

    @Override
    protected void reloadCache() {
        //reload cacheRef and replace to old cacheRef
        Map<String, List<Map<String, Object>>> newCache = Maps.newConcurrentMap();
        try {
            loadData(newCache);
        } catch (SQLException e) {
            LOG.error("", e);
        }

        cacheRef.set(newCache);
        LOG.info("----- cassandra all cacheRef reload end:{}", Calendar.getInstance());
    }


    @Override
    public void flatMap(Row value, Collector<Row> out) throws Exception {
        List<Object> inputParams = Lists.newArrayList();
        for (Integer conValIndex : sideInfo.getEqualValIndex()) {
            Object equalObj = value.getField(conValIndex);
            if (equalObj == null) {
                out.collect(null);
            }

            inputParams.add(equalObj);
        }

        String key = buildKey(inputParams);
        List<Map<String, Object>> cacheList = cacheRef.get().get(key);
        if (CollectionUtils.isEmpty(cacheList)) {
            if (sideInfo.getJoinType() == JoinType.LEFT) {
                Row row = fillData(value, null);
                out.collect(row);
            } else {
                return;
            }

            return;
        }

        for (Map<String, Object> one : cacheList) {
            out.collect(fillData(value, one));
        }

    }

    private String buildKey(List<Object> equalValList) {
        StringBuilder sb = new StringBuilder("");
        for (Object equalVal : equalValList) {
            sb.append(equalVal).append("_");
        }

        return sb.toString();
    }

    private String buildKey(Map<String, Object> val, List<String> equalFieldList) {
        StringBuilder sb = new StringBuilder("");
        for (String equalField : equalFieldList) {
            sb.append(val.get(equalField)).append("_");
        }

        return sb.toString();
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

                for (String server : address.split(",")) {
                    cassandraPort = Integer.parseInt(server.split(":")[1]);
                    serversList.add(InetAddress.getByName(server.split(":")[0]));
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


    private void loadData(Map<String, List<Map<String, Object>>> tmpCache) throws SQLException {
        CassandraSideTableInfo tableInfo = (CassandraSideTableInfo) sideInfo.getSideTableInfo();
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
                        Thread.sleep(5 * 1000);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }

            }

            //load data from table
            String sql = sideInfo.getSqlCondition() + " limit " + FETCH_SIZE;
            ResultSet resultSet = session.execute(sql);
            String[] sideFieldNames = sideInfo.getSideSelectFields().split(",");
            for (com.datastax.driver.core.Row row : resultSet) {
                Map<String, Object> oneRow = Maps.newHashMap();
                for (String fieldName : sideFieldNames) {
                    oneRow.put(fieldName.trim(), row.getObject(fieldName.trim()));
                }
                String cacheKey = buildKey(oneRow, sideInfo.getEqualFieldList());
                List<Map<String, Object>> list = tmpCache.computeIfAbsent(cacheKey, key -> Lists.newArrayList());
                list.add(oneRow);
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
