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
import com.dtstack.flink.sql.side.CacheMissVal;
import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.rdb.table.RdbSideTableInfo;
import com.dtstack.flink.sql.side.rdb.util.SwitchUtil;
import com.dtstack.flink.sql.util.DateUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;

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

    public final static int MAX_DB_CONN_POOL_SIZE_LIMIT = 20;

    public final static int DEFAULT_IDLE_CONNECTION_TEST_PEROID = 60;

    public final static boolean DEFAULT_TEST_CONNECTION_ON_CHECKIN = true;

    public final static String DT_PROVIDER_CLASS = "com.dtstack.flink.sql.side.rdb.provider.DTC3P0DataSourceProvider";

    public final static String PREFERRED_TEST_QUERY_SQL = "select 1 from dual";

    private transient SQLClient rdbSqlClient;

    public RdbAsyncReqRow(BaseSideInfo sideInfo) {
        super(sideInfo);
        init(sideInfo);
    }

    protected void init(BaseSideInfo sideInfo) {
        RdbSideTableInfo rdbSideTableInfo = (RdbSideTableInfo) sideInfo.getSideTableInfo();
        int defaultAsyncPoolSize = Math.min(MAX_DB_CONN_POOL_SIZE_LIMIT, DEFAULT_DB_CONN_POOL_SIZE);
        int rdbPoolSize = rdbSideTableInfo.getAsyncPoolSize() > 0 ? rdbSideTableInfo.getAsyncPoolSize() : defaultAsyncPoolSize;
        rdbSideTableInfo.setAsyncPoolSize(rdbPoolSize);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RdbSideTableInfo rdbSideTableInfo = (RdbSideTableInfo) sideInfo.getSideTableInfo();
        LOG.info("rdb dim table config info: {} ", rdbSideTableInfo.toString());
    }

    @Override
    public void asyncInvoke(CRow input, ResultFuture<CRow> resultFuture) throws Exception {
        CRow copyCrow = new CRow(input.row(), input.change());
        JsonArray inputParams = new JsonArray();
        for (Integer conValIndex : sideInfo.getEqualValIndex()) {
            Object equalObj = copyCrow.row().getField(conValIndex);
            if (equalObj == null) {
                dealMissKey(copyCrow, resultFuture);
                return;
            }
            inputParams.add(convertDataType(equalObj));
        }

        String key = buildCacheKey(inputParams);
        if (openCache()) {
            CacheObj val = getFromCache(key);
            if (val != null) {
                if (ECacheContentType.MissVal == val.getType()) {
                    dealMissKey(copyCrow, resultFuture);
                    return;
                } else if (ECacheContentType.MultiLine == val.getType()) {
                    try {
                        List<CRow> rowList = getRows(copyCrow, null, (List) val.getContent());
                        resultFuture.complete(rowList);
                    } catch (Exception e) {
                        dealFillDataError(resultFuture, e, copyCrow);
                    }
                } else {
                    resultFuture.completeExceptionally(new RuntimeException("not support cache obj type " + val.getType()));
                }
                return;
            }
        }

        rdbSQLClient.getConnection(conn -> {
            if (conn.failed()) {
                //Treatment failures
                resultFuture.completeExceptionally(conn.cause());
                return;
            }

            final SQLConnection connection = conn.result();
            String sqlCondition = sideInfo.getSqlCondition();
            connection.queryWithParams(sqlCondition, inputParams, rs -> {
                if (rs.failed()) {
                    LOG.error("Cannot retrieve the data from the database", rs.cause());
                    resultFuture.completeExceptionally(rs.cause());
                    return;
                }
                List<JsonArray> cacheContent = Lists.newArrayList();
                List<JsonArray> results = rs.result().getResults();
                if (results.size() > 0) {
                    try {
                        List<CRow> rowList = getRows(copyCrow, cacheContent, results);
                        dealCacheData(key, CacheObj.buildCacheObj(ECacheContentType.MultiLine, cacheContent));
                        resultFuture.complete(rowList);
                    } catch (Exception e){
                        dealFillDataError(resultFuture, e, copyCrow);
                    }
                } else {
                    dealMissKey(copyCrow, resultFuture);
                    dealCacheData(key, CacheMissVal.getMissKeyObj());
                }

                // and close the connection
                connection.close(done -> {
                    if (done.failed()) {
                        throw new RuntimeException(done.cause());
                    }
                });
            });
        });
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
            val = DateUtil.getStringFromTimestamp((Timestamp) val);
        } else if (val instanceof java.util.Date) {
            val = DateUtil.getStringFromDate((java.sql.Date) val);
        } else {
            val = val.toString();
        }
        return val;
    }

    protected List<CRow> getRows(CRow inputRow, List<JsonArray> cacheContent, List<JsonArray> results) {
        List<CRow> rowList = Lists.newArrayList();
        for (JsonArray line : results) {
            Row row = fillData(inputRow.row(), line);
            if (null != cacheContent && openCache()) {
                cacheContent.add(line);
            }
            rowList.add(new CRow(row, inputRow.change()));
        }
        return rowList;
    }

    @Override
    public Row fillData(Row input, Object line) {
        JsonArray jsonArray = (JsonArray) line;
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
        if (rdbSqlClient != null) {
            rdbSqlClient.close();
        }

    }

    public String buildCacheKey(JsonArray jsonArray) {
        StringBuilder sb = new StringBuilder();
        for (Object ele : jsonArray.getList()) {
            sb.append(ele.toString())
                    .append("_");
        }

        return sb.toString();
    }

    public void setRdbSqlClient(SQLClient rdbSqlClient) {
        this.rdbSqlClient = rdbSqlClient;
    }

}
