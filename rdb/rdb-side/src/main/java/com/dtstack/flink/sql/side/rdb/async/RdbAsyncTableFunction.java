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

package com.dtstack.flink.sql.side.rdb.async;

import com.dtstack.flink.sql.enums.ECacheContentType;
import com.dtstack.flink.sql.factory.DTThreadFactory;
import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.CacheMissVal;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.rdb.table.RdbSideTableInfo;
import com.dtstack.flink.sql.side.rdb.util.SwitchUtil;
import com.dtstack.flink.sql.side.table.BaseAsyncTableFunction;
import com.dtstack.flink.sql.util.DateUtil;
import com.google.common.collect.Lists;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author: chuixue
 * @create: 2020-10-12 19:00
 * @description:Rdb异步维表公共的类
 **/
public class RdbAsyncTableFunction extends BaseAsyncTableFunction {

    private static final long serialVersionUID = 2098635244857937720L;

    private static final Logger LOG = LoggerFactory.getLogger(RdbAsyncTableFunction.class);

    public final static int DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE = 1;

    public final static int DEFAULT_VERTX_WORKER_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;

    public final static int DEFAULT_DB_CONN_POOL_SIZE = DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE + DEFAULT_VERTX_WORKER_POOL_SIZE;

    public final static int MAX_DB_CONN_POOL_SIZE_LIMIT = 20;

    public final static int DEFAULT_IDLE_CONNECTION_TEST_PEROID = 60;

    public final static boolean DEFAULT_TEST_CONNECTION_ON_CHECKIN = true;

    public final static String DT_PROVIDER_CLASS = "com.dtstack.flink.sql.side.rdb.provider.DTC3P0DataSourceProvider";

    public final static String PREFERRED_TEST_QUERY_SQL = "SELECT 1 FROM DUAL";

    private transient SQLClient rdbSqlClient;

    private AtomicBoolean connectionStatus = new AtomicBoolean(true);

    private transient ThreadPoolExecutor executor;

    private final static int MAX_TASK_QUEUE_SIZE = 100000;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        executor = new ThreadPoolExecutor(MAX_DB_CONN_POOL_SIZE_LIMIT, MAX_DB_CONN_POOL_SIZE_LIMIT, 0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(MAX_TASK_QUEUE_SIZE), new DTThreadFactory("rdbAsyncExec"), new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public RdbAsyncTableFunction(BaseSideInfo sideInfo) {
        super(sideInfo);
        init(sideInfo);
    }

    protected void init(BaseSideInfo sideInfo) {
        RdbSideTableInfo rdbSideTableInfo = (RdbSideTableInfo) sideTableInfo;
        int defaultAsyncPoolSize = Math.min(MAX_DB_CONN_POOL_SIZE_LIMIT, DEFAULT_DB_CONN_POOL_SIZE);
        int rdbPoolSize = rdbSideTableInfo.getAsyncPoolSize() > 0 ? rdbSideTableInfo.getAsyncPoolSize() : defaultAsyncPoolSize;
        rdbSideTableInfo.setAsyncPoolSize(rdbPoolSize);
    }

    @Override
    public void handleAsyncInvoke(CompletableFuture<Collection<Row>> future, Object... keys) throws Exception {
        AtomicLong networkLogCounter = new AtomicLong(0L);
        //network is unhealthy
        while (!connectionStatus.get()) {
            if (networkLogCounter.getAndIncrement() % 1000 == 0) {
                LOG.info("network unhealth to block task");
            }
            Thread.sleep(100);
        }
        List<Object> convertKeys = Stream.of(keys)
                .map(key -> convertDataType(key))
                .collect(Collectors.toList());
        executor.execute(() -> connectWithRetry(future, rdbSqlClient, convertKeys.toArray(new Object[convertKeys.size()])));
    }

    /**
     * 执行异步查询
     *
     * @param future
     * @param rdbSqlClient 数据库客户端
     * @param failCounter  失败次数
     * @param finishFlag   完成标识
     * @param latch        同步标识
     * @param keys         关联字段值
     */
    protected void asyncQueryData(CompletableFuture<Collection<Row>> future,
                                  SQLClient rdbSqlClient,
                                  AtomicLong failCounter,
                                  AtomicBoolean finishFlag,
                                  CountDownLatch latch,
                                  Object... keys) {
        doAsyncQueryData(
                future,
                rdbSqlClient,
                failCounter,
                finishFlag,
                latch,
                keys);
    }

    final protected void doAsyncQueryData(
            CompletableFuture<Collection<Row>> future,
            SQLClient rdbSqlClient,
            AtomicLong failCounter,
            AtomicBoolean finishFlag,
            CountDownLatch latch,
            Object... keys) {
        rdbSqlClient.getConnection(conn -> {
            try {
                if (conn.failed()) {
                    connectionStatus.set(false);
                    if (failCounter.getAndIncrement() % 1000 == 0) {
                        LOG.error("getConnection error", conn.cause());
                    }
                    if (failCounter.get() >= sideTableInfo.getConnectRetryMaxNum(100)) {
                        future.completeExceptionally(conn.cause());
                        finishFlag.set(true);
                    }
                    return;
                }
                connectionStatus.set(true);

                handleQuery(conn.result(), future, keys);
                finishFlag.set(true);
            } catch (Exception e) {
                dealFillDataError(future, e);
            } finally {
                latch.countDown();
            }
        });
    }

    /**
     * @param future
     * @param rdbSqlClient 数据库客户端
     * @param keys         关联字段值
     */
    private void connectWithRetry(CompletableFuture<Collection<Row>> future, SQLClient rdbSqlClient, Object... keys) {
        AtomicLong failCounter = new AtomicLong(0);
        AtomicBoolean finishFlag = new AtomicBoolean(false);
        while (!finishFlag.get()) {
            try {
                CountDownLatch latch = new CountDownLatch(1);
                asyncQueryData(
                        future,
                        rdbSqlClient,
                        failCounter,
                        finishFlag,
                        latch,
                        keys);
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    LOG.error("", e);
                }

            } catch (Exception e) {
                //数据源队列溢出情况
                connectionStatus.set(false);
            }
            if (!finishFlag.get()) {
                try {
                    Thread.sleep(3000);
                } catch (Exception e) {
                    LOG.error("", e);
                }
            }
        }
    }

    /**
     * 数据类型转换
     *
     * @param val 原始类型
     * @return 结果类型
     */
    private Object convertDataType(Object val) {
        if (val == null) {
            // OK
        } else if (val instanceof Number && !(val instanceof BigDecimal)) {
            // OK
        } else if (val instanceof Boolean) {
            // OK
        } else if (val instanceof String) {
            // OK
        } else if (val instanceof Character) {
            // OK
        } else if (val instanceof CharSequence) {

        } else if (val instanceof JsonObject) {

        } else if (val instanceof JsonArray) {

        } else if (val instanceof Map) {

        } else if (val instanceof List) {

        } else if (val instanceof byte[]) {

        } else if (val instanceof Instant) {

        } else if (val instanceof Timestamp) {
            val = DateUtil.timestampToString((Timestamp) val);
        } else if (val instanceof java.util.Date) {
            val = DateUtil.dateToString((java.sql.Date) val);
        } else {
            val = val.toString();
        }
        return val;

    }

    @Override
    protected void fillDataWapper(Object sideInput, String[] sideFieldNames, String[] sideFieldTypes, Row row) {
        JsonArray jsonArray = (JsonArray) sideInput;
        for (int i = 0; i < sideFieldNames.length; i++) {
            String fieldType = sideFieldTypes[i];
            Object object = SwitchUtil.getTarget(jsonArray.getValue(i), fieldType);
            row.setField(i, object);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (rdbSqlClient != null) {
            rdbSqlClient.close();
        }

        if (executor != null) {
            executor.shutdown();
        }

    }

    public void setRdbSqlClient(SQLClient rdbSqlClient) {
        this.rdbSqlClient = rdbSqlClient;
    }

    /**
     * 执行异步查询
     *
     * @param connection 连接
     * @param future
     * @param keys       关联健值
     */
    private void handleQuery(SQLConnection connection, CompletableFuture<Collection<Row>> future, Object... keys) {
        String cacheKey = buildCacheKey(keys);
        JsonArray params = new JsonArray();
        Stream.of(keys).forEach(key -> params.add(key));
        connection.queryWithParams(sideInfo.getFlinkPlannerSqlCondition(), params, rs -> {
            if (rs.failed()) {
                dealFillDataError(future, rs.cause());
                return;
            }

            List<JsonArray> cacheContent = Lists.newArrayList();

            int resultSize = rs.result().getResults().size();
            if (resultSize > 0) {
                List<Row> rowList = Lists.newArrayList();

                for (JsonArray line : rs.result().getResults()) {
                    Row row = fillData(line);
                    if (openCache()) {
                        cacheContent.add(line);
                    }
                    rowList.add(row);
                }

                if (openCache()) {
                    putCache(cacheKey, CacheObj.buildCacheObj(ECacheContentType.MultiLine, cacheContent));
                }
                future.complete(rowList);
            } else {
                dealMissKey(future);
                if (openCache()) {
                    putCache(cacheKey, CacheMissVal.getMissKeyObj());
                }
            }

            // and close the connection
            connection.close(done -> {
                if (done.failed()) {
                    throw new RuntimeException(done.cause());
                }
            });
        });
    }
}
