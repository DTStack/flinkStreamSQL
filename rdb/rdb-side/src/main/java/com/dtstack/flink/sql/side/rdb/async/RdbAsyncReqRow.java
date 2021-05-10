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

import com.dtstack.flink.sql.core.rdb.JdbcResourceCheck;
import com.dtstack.flink.sql.enums.ECacheContentType;
import com.dtstack.flink.sql.exception.ExceptionTrace;
import com.dtstack.flink.sql.factory.DTThreadFactory;
import com.dtstack.flink.sql.side.BaseAsyncReqRow;
import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.CacheMissVal;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.rdb.table.RdbSideTableInfo;
import com.dtstack.flink.sql.side.rdb.util.SwitchUtil;
import com.dtstack.flink.sql.util.DateUtil;
import com.dtstack.flink.sql.util.RowDataComplete;
import com.dtstack.flink.sql.util.ThreadUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Date: 2018/11/26
 * Company: www.dtstack.com
 *
 * @author maqi
 */

public class RdbAsyncReqRow extends BaseAsyncReqRow {

    private static final long serialVersionUID = 2098635244857937720L;

    private static final Logger LOG = LoggerFactory.getLogger(RdbAsyncReqRow.class);

    public final static int DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE = 1;

    public final static int DEFAULT_VERTX_WORKER_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;

    public final static int DEFAULT_DB_CONN_POOL_SIZE = DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE + DEFAULT_VERTX_WORKER_POOL_SIZE;

    public final static int MAX_DB_CONN_POOL_SIZE_LIMIT = 5;

    public final static int DEFAULT_IDLE_CONNECTION_TEST_PEROID = 60;

    public final static boolean DEFAULT_TEST_CONNECTION_ON_CHECKIN = true;

    public final static String DT_PROVIDER_CLASS = "com.dtstack.flink.sql.side.rdb.provider.DTC3P0DataSourceProvider";

    public final static String PREFERRED_TEST_QUERY_SQL = "SELECT 1 FROM DUAL";

    private transient SQLClient rdbSqlClient;

    private transient Vertx vertx;

    private int asyncPoolSize = 1;

    private final int errorLogPrintNum = 3;

    private final AtomicBoolean connectionStatus = new AtomicBoolean(true);

    private static volatile boolean resourceCheck = false;

    private transient ThreadPoolExecutor executor;

    // 共享条件：1.一个维表多个并行度在一个tm上可以共享，2.多个同种类型维表单个并行度、多个并行度在一个tm上可以共享
    protected static Map<String, SQLClient> rdbSqlClientPool = Maps.newConcurrentMap();
    // 同种类型维表以url为单位，在同一个tm中只有一个连接池
    private String url ;

    private final static int MAX_TASK_QUEUE_SIZE = 100000;

    public RdbAsyncReqRow(BaseSideInfo sideInfo) {
        super(sideInfo);
        init(sideInfo);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        RdbSideTableInfo rdbSideTableInfo = (RdbSideTableInfo) sideInfo.getSideTableInfo();
        synchronized (RdbAsyncReqRow.class) {
            if (resourceCheck) {
                resourceCheck = false;
                JdbcResourceCheck.getInstance().checkResourceStatus(rdbSideTableInfo.getCheckProperties());
            }
        }
        super.open(parameters);

        VertxOptions vertxOptions = new VertxOptions();
        if (clientShare) {
            rdbSideTableInfo.setAsyncPoolSize(poolSize);
        }

        JsonObject jdbcConfig = buildJdbcConfig();
        System.setProperty("vertx.disableFileCPResolving", "true");
        vertxOptions
                .setEventLoopPoolSize(DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE)
                .setWorkerPoolSize(asyncPoolSize)
                .setFileResolverCachingEnabled(false);

        executor = new ThreadPoolExecutor(
                MAX_DB_CONN_POOL_SIZE_LIMIT,
                MAX_DB_CONN_POOL_SIZE_LIMIT,
                0,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(MAX_TASK_QUEUE_SIZE),
                new DTThreadFactory("rdbAsyncExec"),
                new ThreadPoolExecutor.CallerRunsPolicy());

        vertx = Vertx.vertx(vertxOptions);
        if (clientShare) {
            rdbSqlClientPool.putIfAbsent(url, JDBCClient.createNonShared(vertx, jdbcConfig));
            LOG.info("{} lru type open connection share, url:{}, rdbSqlClientPool size:{}, rdbSqlClientPool:{}", rdbSideTableInfo.getType(), url, rdbSqlClientPool.size(), rdbSqlClientPool);
        } else {
            this.rdbSqlClient = JDBCClient.createNonShared(vertx, jdbcConfig);
        }

    }

    protected void init(BaseSideInfo sideInfo) {
        RdbSideTableInfo rdbSideTableInfo = (RdbSideTableInfo) sideInfo.getSideTableInfo();
        int defaultAsyncPoolSize = Math.min(MAX_DB_CONN_POOL_SIZE_LIMIT, DEFAULT_DB_CONN_POOL_SIZE);
        asyncPoolSize = rdbSideTableInfo.getAsyncPoolSize() > 0 ?
                rdbSideTableInfo.getAsyncPoolSize() : defaultAsyncPoolSize;
        rdbSideTableInfo.setAsyncPoolSize(asyncPoolSize);
        url = rdbSideTableInfo.getUrl();
    }

    public JsonObject buildJdbcConfig() {
        throw new SuppressRestartsException(
                new Throwable("Function buildJdbcConfig() must be overridden"));
    }

    @Override
    protected void preInvoke(BaseRow input, ResultFuture<BaseRow> resultFuture) {
    }

    @Override
    public void handleAsyncInvoke(Map<String, Object> inputParams, BaseRow input, ResultFuture<BaseRow> resultFuture) throws Exception {
        SQLClient sqlClient = clientShare ? rdbSqlClientPool.get(url) : this.rdbSqlClient;
        AtomicLong networkLogCounter = new AtomicLong(0L);
        //network is unhealthy
        while (!connectionStatus.get()) {
            if (networkLogCounter.getAndIncrement() % 1000 == 0) {
                LOG.info("network unhealthy to block task");
            }
            Thread.sleep(100);
        }
        Map<String, Object> params = formatInputParam(inputParams);
        executor.execute(() -> connectWithRetry(params, input, resultFuture, sqlClient));
    }

    protected void asyncQueryData(Map<String, Object> inputParams,
                                  BaseRow input,
                                  ResultFuture<BaseRow> resultFuture,
                                  SQLClient rdbSqlClient,
                                  AtomicLong failCounter,
                                  AtomicBoolean finishFlag,
                                  CountDownLatch latch) {
        doAsyncQueryData(
                inputParams,
                input,
                resultFuture,
                rdbSqlClient,
                failCounter,
                finishFlag,
                latch);
    }

    final protected void doAsyncQueryData(
            Map<String, Object> inputParams,
            BaseRow input,
            ResultFuture<BaseRow> resultFuture,
            SQLClient rdbSqlClient,
            AtomicLong failCounter,
            AtomicBoolean finishFlag,
            CountDownLatch latch) {
        rdbSqlClient.getConnection(conn -> {
            try {
                String errorMsg;
                Integer retryMaxNum = sideInfo.getSideTableInfo().getConnectRetryMaxNum(3);
                int logPrintTime = retryMaxNum / errorLogPrintNum == 0 ?
                        retryMaxNum : retryMaxNum / errorLogPrintNum;
                if (conn.failed()) {
                    connectionStatus.set(false);
                    errorMsg = ExceptionTrace.traceOriginalCause(conn.cause());
                    if (failCounter.getAndIncrement() % logPrintTime == 0) {
                        LOG.error("getConnection error. cause by " + errorMsg);
                    }
                    LOG.error(String.format("retry ... current time [%s]", failCounter.get()));
                    if (failCounter.get() >= retryMaxNum) {
                        resultFuture.completeExceptionally(
                                new SuppressRestartsException(conn.cause())
                        );
                        finishFlag.set(true);
                    }
                    return;
                }
                connectionStatus.set(true);
                registerTimerAndAddToHandler(input, resultFuture);

                handleQuery(conn.result(), inputParams, input, resultFuture);
                finishFlag.set(true);
            } catch (Exception e) {
                dealFillDataError(input, resultFuture, e);
            } finally {
                latch.countDown();
            }
        });
    }

    private void connectWithRetry(Map<String, Object> inputParams, BaseRow input, ResultFuture<BaseRow> resultFuture, SQLClient rdbSqlClient) {
        AtomicLong failCounter = new AtomicLong(0);
        AtomicBoolean finishFlag = new AtomicBoolean(false);
        while (!finishFlag.get()) {
            try {
                CountDownLatch latch = new CountDownLatch(1);
                asyncQueryData(
                        inputParams,
                        input,
                        resultFuture,
                        rdbSqlClient,
                        failCounter,
                        finishFlag,
                        latch);
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
                ThreadUtil.sleepSeconds(ThreadUtil.DEFAULT_SLEEP_TIME);
            }
        }
    }

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
    public String buildCacheKey(Map<String, Object> inputParam) {
        return StringUtils.join(inputParam.values(), "_");
    }

    @Override
    public BaseRow fillData(BaseRow input, Object line) {
        GenericRow genericRow = (GenericRow) input;
        JsonArray jsonArray = (JsonArray) line;
        GenericRow row = new GenericRow(sideInfo.getOutFieldInfoList().size());
        row.setHeader(input.getHeader());
        for (Map.Entry<Integer, Integer> entry : sideInfo.getInFieldIndex().entrySet()) {
            Object obj = genericRow.getField(entry.getValue());
            obj = convertTimeIndictorTypeInfo(entry.getValue(), obj);
            row.setField(entry.getKey(), obj);
        }

        for (Map.Entry<Integer, Integer> entry : sideInfo.getSideFieldIndex().entrySet()) {
            if (jsonArray == null) {
                row.setField(entry.getKey(), null);
            } else {
                String fieldType = sideInfo.getSelectSideFieldType(entry.getValue());
                Object object = SwitchUtil.getTarget(jsonArray.getValue(entry.getValue()), fieldType);
                row.setField(entry.getKey(), object);
            }
        }

        return row;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (clientShare && rdbSqlClientPool.get(url) != null) {
            rdbSqlClientPool.get(url).close(getAsyncResultHandler());
        }
        if (rdbSqlClient != null) {
            rdbSqlClient.close(getAsyncResultHandler());
        }

        if (executor != null) {
            executor.shutdown();
        }

        // 关闭异步连接vertx事件循环线程，因为vertx使用的是非守护线程
        if (Objects.nonNull(vertx)) {
            vertx.close(done -> {
                if (done.failed()) {
                    LOG.error("vert.x close error. cause by " +
                            ExceptionTrace.traceOriginalCause(done.cause())
                    );
                }
            });
        }
    }

    private Handler<AsyncResult<Void>> getAsyncResultHandler() {
        return done -> {
            if (done.failed()) {
                LOG.error("sql client close failed! " +
                        ExceptionTrace.traceOriginalCause(done.cause())
                );
            }

            if (done.succeeded()) {
                LOG.info("sql client closed.");
            }
        };
    }

    private void handleQuery(SQLConnection connection, Map<String, Object> inputParams, BaseRow input, ResultFuture<BaseRow> resultFuture) {
        String key = buildCacheKey(inputParams);
        JsonArray params = new JsonArray(Lists.newArrayList(inputParams.values()));
        connection.queryWithParams(sideInfo.getSqlCondition(), params, rs -> {
            try {
                if (rs.failed()) {
                    LOG.error(
                            String.format("\nget data with sql [%s] failed! \ncause: [%s]",
                                    sideInfo.getSqlCondition(),
                                    rs.cause().getMessage()
                            )
                    );
                    dealFillDataError(input, resultFuture, rs.cause());
                    return;
                }

                List<JsonArray> cacheContent = Lists.newArrayList();

                int resultSize = rs.result().getResults().size();
                if (resultSize > 0) {
                    List<BaseRow> rowList = Lists.newArrayList();

                    for (JsonArray line : rs.result().getResults()) {
                        BaseRow row = fillData(input, line);
                        if (openCache()) {
                            cacheContent.add(line);
                        }
                        rowList.add(row);
                    }

                    if (openCache()) {
                        putCache(key, CacheObj.buildCacheObj(ECacheContentType.MultiLine, cacheContent));
                    }
                    RowDataComplete.completeBaseRow(resultFuture, rowList);
                } else {
                    dealMissKey(input, resultFuture);
                    if (openCache()) {
                        putCache(key, CacheMissVal.getMissKeyObj());
                    }
                }
            } finally {
                // and close the connection
                connection.close(done -> {
                    if (done.failed()) {
                        LOG.error("sql connection close failed! " +
                                ExceptionTrace.traceOriginalCause(done.cause())
                        );
                    }
                });
            }
        });
    }

    private Map<String, Object> formatInputParam(Map<String, Object> inputParam) {
        Map<String, Object> result = Maps.newLinkedHashMap();
        inputParam.forEach((k, v) -> {
            result.put(k, convertDataType(v));
        });
        return result;
    }
}
