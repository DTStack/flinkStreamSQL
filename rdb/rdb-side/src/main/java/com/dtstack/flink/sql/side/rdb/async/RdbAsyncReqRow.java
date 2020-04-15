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
import com.dtstack.flink.sql.side.BaseAsyncReqRow;
import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.CacheMissVal;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.rdb.util.SwitchUtil;
import com.google.common.collect.Lists;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
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

    public final static int DEFAULT_MAX_DB_CONN_POOL_SIZE = DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE + DEFAULT_VERTX_WORKER_POOL_SIZE;

    public final static int DEFAULT_IDLE_CONNECTION_TEST_PEROID = 60;

    public final static boolean DEFAULT_TEST_CONNECTION_ON_CHECKIN = true;

    public final static String DT_PROVIDER_CLASS = "com.dtstack.flink.sql.side.rdb.provider.DTC3P0DataSourceProvider";

    public final static String PREFERRED_TEST_QUERY_SQL = "select 1 from dual";

    private transient SQLClient rdbSqlClient;

    private final static AtomicBoolean CONN_STATUS = new AtomicBoolean(true);

    private Logger logger = LoggerFactory.getLogger(getClass());

    public RdbAsyncReqRow(BaseSideInfo sideInfo) {
        super(sideInfo);
    }


    @Override
    protected void preInvoke(CRow input, ResultFuture<CRow> resultFuture){

    }

    @Override
    public void handleAsyncInvoke(Map<String, Object> inputParams, CRow input, ResultFuture<CRow> resultFuture) throws Exception {

        AtomicLong networkLogCounter = new AtomicLong(0L);
        while (!CONN_STATUS.get()){//network is unhealth
            //todo:统一计数
            if(networkLogCounter.getAndIncrement() % 1000 == 0){
                LOG.info("network unhealth to block task");
            }
            Thread.sleep(100);
        }
        rdbSqlClient.getConnection(conn -> {
            if(conn.failed()){
                CONN_STATUS.set(false);
                connectWithRetry(inputParams, input, resultFuture, rdbSqlClient);
                return;
            }
            CONN_STATUS.set(true);
            ScheduledFuture<?> timerFuture = registerTimer(input, resultFuture);
            cancelTimerWhenComplete(resultFuture, timerFuture);
            handleQuery(conn.result(), inputParams, input, resultFuture);
        });

    }

    private void connectWithRetry(Map<String, Object> inputParams, CRow input, ResultFuture<CRow> resultFuture, SQLClient rdbSqlClient) {
        AtomicLong failCounter = new AtomicLong(0);
        AtomicBoolean finishFlag = new AtomicBoolean(false);
        while(!finishFlag.get()){
            CountDownLatch latch = new CountDownLatch(1);
            rdbSqlClient.getConnection(conn -> {
                try {
                    if(conn.failed()){
                        if(failCounter.getAndIncrement() % 1000 == 0){
                            logger.error("getConnection error", conn.cause());
                        }
                        if(failCounter.get() >= sideInfo.getSideTableInfo().getAsyncFailMaxNum(3L)){
                            dealFillDataError(input, resultFuture, conn.cause());
                            finishFlag.set(true);
                        }
                        conn.result().close();
                        return;
                    }
                    CONN_STATUS.set(true);
                    ScheduledFuture<?> timerFuture = registerTimer(input, resultFuture);
                    cancelTimerWhenComplete(resultFuture, timerFuture);
                    handleQuery(conn.result(), inputParams, input, resultFuture);
                    finishFlag.set(true);
                } catch (Exception e) {
                    dealFillDataError(input, resultFuture, e);
                } finally {
                    latch.countDown();
                }
            });
            //主线程阻塞
            try {
                latch.wait();
            } catch (InterruptedException e) {
                logger.error("", e);
            }
        }

    }

    @Override
    public String buildCacheKey(Map<String, Object> inputParam) {
        return StringUtils.join(inputParam.values(),"_");
    }

    @Override
    public Row fillData(Row input, Object line) {
        JsonArray jsonArray = (JsonArray) line;
        Row row = new Row(sideInfo.getOutFieldInfoList().size());
        String[] fields = sideInfo.getSideTableInfo().getFieldTypes();
        for (Map.Entry<Integer, Integer> entry : sideInfo.getInFieldIndex().entrySet()) {
            Object obj = input.getField(entry.getValue());
            boolean isTimeIndicatorTypeInfo = TimeIndicatorTypeInfo.class.isAssignableFrom(sideInfo.getRowTypeInfo().getTypeAt(entry.getValue()).getClass());
            if (obj instanceof Timestamp && isTimeIndicatorTypeInfo) {
                obj = ((Timestamp) obj).getTime();
            }

            row.setField(entry.getKey(), obj);
        }

        for (Map.Entry<Integer, Integer> entry : sideInfo.getSideFieldIndex().entrySet()) {
            if (jsonArray == null) {
                row.setField(entry.getKey(), null);
            } else {
                Object object = SwitchUtil.getTarget(jsonArray.getValue(entry.getValue()), fields[entry.getValue()]);
                row.setField(entry.getKey(), object);
            }
        }

        return row;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (rdbSqlClient != null) {
            rdbSqlClient.close();
        }

    }

    public void setRdbSqlClient(SQLClient rdbSqlClient) {
        this.rdbSqlClient = rdbSqlClient;
    }

    private void handleQuery(SQLConnection connection,Map<String, Object> inputParams, CRow input, ResultFuture<CRow> resultFuture){
        String key = buildCacheKey(inputParams);
        JsonArray params = new JsonArray(Lists.newArrayList(inputParams.values()));
        connection.queryWithParams(sideInfo.getSqlCondition(), params, rs -> {
            if (rs.failed()) {
                dealFillDataError(input, resultFuture, rs.cause());
                return;
            }

            List<JsonArray> cacheContent = Lists.newArrayList();

            int resultSize = rs.result().getResults().size();
            if (resultSize > 0) {
                List<CRow> rowList = Lists.newArrayList();

                for (JsonArray line : rs.result().getResults()) {
                    Row row = fillData(input.row(), line);
                    if (openCache()) {
                        cacheContent.add(line);
                    }
                    rowList.add(new CRow(row, input.change()));
                }

                if (openCache()) {
                    putCache(key, CacheObj.buildCacheObj(ECacheContentType.MultiLine, cacheContent));
                }

                resultFuture.complete(rowList);
            } else {
                dealMissKey(input, resultFuture);
                if (openCache()) {
                    putCache(key, CacheMissVal.getMissKeyObj());
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
