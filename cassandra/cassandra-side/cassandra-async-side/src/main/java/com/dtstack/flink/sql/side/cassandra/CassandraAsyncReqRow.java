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

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;

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
import com.dtstack.flink.sql.enums.ECacheContentType;
import com.dtstack.flink.sql.side.BaseAsyncReqRow;
import com.dtstack.flink.sql.side.CacheMissVal;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.cassandra.table.CassandraSideTableInfo;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.vertx.core.json.JsonArray;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Reason:
 * Date: 2018/11/22
 *
 * @author xuqianjin
 */
public class CassandraAsyncReqRow extends BaseAsyncReqRow {

    private static final long serialVersionUID = 6631584128079864735L;

    private static final Logger LOG = LoggerFactory.getLogger(CassandraAsyncReqRow.class);

    private final static int DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE = 10;

    private final static int DEFAULT_VERTX_WORKER_POOL_SIZE = 20;

    private final static int DEFAULT_MAX_DB_CONN_POOL_SIZE = 20;

    private transient Cluster cluster;
    private transient ListenableFuture session;
    private transient CassandraSideTableInfo cassandraSideTableInfo;

    public CassandraAsyncReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, AbstractSideTableInfo sideTableInfo) {
        super(new com.dtstack.flink.sql.side.cassandra.CassandraAsyncSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        cassandraSideTableInfo = (CassandraSideTableInfo) sideInfo.getSideTableInfo();
        connCassandraDB(cassandraSideTableInfo);
    }

    private void connCassandraDB(CassandraSideTableInfo tableInfo) {
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
                session = cluster.connectAsync(database);
                LOG.info("connect cassandra is successed!");
            }
        } catch (Exception e) {
            LOG.error("connect cassandra is error:" + e.getMessage());
        }
    }

    @Override
    public void handleAsyncInvoke(Map<String, Object> inputParams, CRow input, ResultFuture<CRow> resultFuture) throws Exception {

        String key = buildCacheKey(inputParams);
        //connect Cassandra
        connCassandraDB(cassandraSideTableInfo);

        String sqlCondition = sideInfo.getSqlCondition() + " " + buildWhereCondition(inputParams) + "  ALLOW FILTERING ";

        ListenableFuture<ResultSet> resultSet = Futures.transformAsync(session,
                new AsyncFunction<Session, ResultSet>() {
                    @Override
                    public ListenableFuture<ResultSet> apply(Session session) throws Exception {
                        return session.executeAsync(sqlCondition);
                    }
                });

        ListenableFuture<List<com.datastax.driver.core.Row>> data = Futures.transform(resultSet,
                new Function<ResultSet, List<com.datastax.driver.core.Row>>() {
                    @Override
                    public List<com.datastax.driver.core.Row> apply(ResultSet rs) {
                        return rs.all();
                    }
                });

        Futures.addCallback(data, new FutureCallback<List<com.datastax.driver.core.Row>>() {
            @Override
            public void onSuccess(List<com.datastax.driver.core.Row> rows) {
                cluster.closeAsync();
                if (rows.size() > 0) {
                    List<com.datastax.driver.core.Row> cacheContent = Lists.newArrayList();
                    List<CRow> rowList = Lists.newArrayList();
                    for (com.datastax.driver.core.Row line : rows) {
                        Row row = fillData(input.row(), line);
                        if (openCache()) {
                            cacheContent.add(line);
                        }
                        rowList.add(new CRow(row, input.change()));
                    }
                    resultFuture.complete(rowList);
                    if (openCache()) {
                        putCache(key, CacheObj.buildCacheObj(ECacheContentType.MultiLine, cacheContent));
                    }
                } else {
                    dealMissKey(input, resultFuture);
                    if (openCache()) {
                        putCache(key, CacheMissVal.getMissKeyObj());
                    }
                    resultFuture.complete(null);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                LOG.error("Failed to retrieve the data: %s%n",
                        t.getMessage());
                cluster.closeAsync();
                resultFuture.completeExceptionally(t);
            }
        });
    }

    @Override
    public String buildCacheKey(Map<String, Object> inputParams) {
        StringBuilder sb = new StringBuilder();
        for (Object ele : inputParams.values()) {
            sb.append(ele.toString()).append("_");
        }
        return sb.toString();
    }

    private String buildWhereCondition(Map<String, Object> inputParams){
        StringBuilder sb = new StringBuilder(" where ");
        for(Map.Entry<String, Object> entry : inputParams.entrySet()){
            Object value = entry.getValue() instanceof String ? "'" + entry.getValue() + "'" : entry.getValue();
            sb.append(String.format("%s = %s", entry.getKey(), value));
        }
        return sb.toString();
    }

    @Override
    public Row fillData(Row input, Object line) {
        com.datastax.driver.core.Row rowArray = (com.datastax.driver.core.Row) line;
        Row row = new Row(sideInfo.getOutFieldInfoList().size());
        for (Map.Entry<Integer, Integer> entry : sideInfo.getInFieldIndex().entrySet()) {
            Object obj = input.getField(entry.getValue());
            boolean isTimeIndicatorTypeInfo = TimeIndicatorTypeInfo.class.isAssignableFrom(sideInfo.getRowTypeInfo().getTypeAt(entry.getValue()).getClass());

            if (obj instanceof Timestamp && isTimeIndicatorTypeInfo) {
                obj = ((Timestamp) obj).getTime();
            }

            row.setField(entry.getKey(), obj);
        }

        for (Map.Entry<Integer, Integer> entry : sideInfo.getSideFieldIndex().entrySet()) {
            if (rowArray == null) {
                row.setField(entry.getKey(), null);
            } else {
                row.setField(entry.getKey(), rowArray.getObject(entry.getValue()));
            }
        }

        return row;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (cluster != null) {
            cluster.close();
            cluster = null;
        }
    }
}
