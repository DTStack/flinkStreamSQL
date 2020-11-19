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
import com.dtstack.flink.sql.enums.ECacheContentType;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.CacheMissVal;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.cassandra.CassandraAsyncSideInfo;
import com.dtstack.flink.sql.side.table.BaseAsyncTableFunction;
import com.dtstack.flink.sql.util.SwitchUtil;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author: chuixue
 * @create: 2020-11-19 16:54
 * @description:
 **/
public class CassandraAsyncTableFunction extends BaseAsyncTableFunction {

    private static final long serialVersionUID = 6631584128079864735L;

    private static final Logger LOG = LoggerFactory.getLogger(CassandraAsyncTableFunction.class);

    private transient Cluster cluster;
    private transient ListenableFuture session;
    private transient CassandraSideTableInfo cassandraSideTableInfo;

    public CassandraAsyncTableFunction(AbstractSideTableInfo sideTableInfo, String[] lookupKeys) {
        super(new CassandraAsyncSideInfo(sideTableInfo, lookupKeys));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        cassandraSideTableInfo = (CassandraSideTableInfo) sideTableInfo;
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

    private String buildWhereCondition(Map<String, Object> inputParams) {
        StringBuilder sb = new StringBuilder(" where ");
        for (Map.Entry<String, Object> entry : inputParams.entrySet()) {
            Object value = entry.getValue() instanceof String ? "'" + entry.getValue() + "'" : entry.getValue();
            sb.append(String.format("%s = %s", entry.getKey(), value));
        }
        return sb.toString();
    }

    @Override
    public void handleAsyncInvoke(CompletableFuture<Collection<Row>> future, Object... keys) throws Exception {
        String key = buildCacheKey(keys);
        //connect Cassandra
        connCassandraDB(cassandraSideTableInfo);

        List<String> lookupKeys = Stream
                .of(sideInfo.getLookupKeys())
                .map(e -> physicalFields.getOrDefault(e, e))
                .collect(Collectors.toList());

        String[] lookupKeysArr = lookupKeys.toArray(new String[lookupKeys.size()]);

        Map<String, Object> inputParams = IntStream
                .range(0, lookupKeys.size())
                .boxed()
                .collect(Collectors.toMap(i -> lookupKeysArr[i], i -> keys[i]));

        String sqlCondition = sideInfo.getSqlCondition() + " " + buildWhereCondition(inputParams) + "  ALLOW FILTERING ";
        LOG.info("sqlCondition:{}" + sqlCondition);

        ListenableFuture<ResultSet> resultSet = Futures.transformAsync(session,
                (AsyncFunction<Session, ResultSet>) session -> session.executeAsync(sqlCondition));

        ListenableFuture<List<com.datastax.driver.core.Row>> data = Futures.transform(resultSet,
                (Function<ResultSet, List<com.datastax.driver.core.Row>>) rs -> rs.all());

        Futures.addCallback(data, new FutureCallback<List<com.datastax.driver.core.Row>>() {
            @Override
            public void onSuccess(List<com.datastax.driver.core.Row> rows) {
                cluster.closeAsync();
                if (rows.size() > 0) {
                    List<com.datastax.driver.core.Row> cacheContent = Lists.newArrayList();
                    List<Row> rowList = Lists.newArrayList();
                    for (com.datastax.driver.core.Row line : rows) {
                        Row row = fillData(line);
                        if (openCache()) {
                            cacheContent.add(line);
                        }
                        rowList.add(row);
                    }
                    future.complete(rowList);
                    if (openCache()) {
                        putCache(key, CacheObj.buildCacheObj(ECacheContentType.MultiLine, cacheContent));
                    }
                } else {
                    dealMissKey(future);
                    if (openCache()) {
                        putCache(key, CacheMissVal.getMissKeyObj());
                    }
                    future.complete(Collections.EMPTY_LIST);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                LOG.error("Failed to retrieve the data: %s%n",
                        t.getMessage());
                cluster.closeAsync();
                future.completeExceptionally(t);
            }
        });
    }

    @Override
    public Row fillData(Object sideInput) {
        com.datastax.driver.core.Row varRow = (com.datastax.driver.core.Row) sideInput;
        Row row = new Row(physicalFields.size());
        if (sideInput != null) {
            String[] sideFieldNames = physicalFields.values().stream().toArray(String[]::new);
            String[] sideFieldTypes = sideTableInfo.getFieldTypes();
            for (int i = 0; i < sideFieldNames.length; i++) {
                row.setField(i, SwitchUtil.getTarget(varRow.getObject(sideFieldNames[i].trim()), sideFieldTypes[i]));
            }
        }
        row.setKind(RowKind.INSERT);
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
