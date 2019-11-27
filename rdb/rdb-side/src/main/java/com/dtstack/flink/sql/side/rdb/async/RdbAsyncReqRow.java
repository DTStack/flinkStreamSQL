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
import com.dtstack.flink.sql.side.*;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.rdb.util.SwitchUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * Date: 2018/11/26
 * Company: www.dtstack.com
 *
 * @author maqi
 */

public class RdbAsyncReqRow extends AsyncReqRow {

    private static final long serialVersionUID = 2098635244857937720L;

    private static final Logger LOG = LoggerFactory.getLogger(RdbAsyncReqRow.class);

    public final static int DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE = 1;

    public final static int DEFAULT_VERTX_WORKER_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;

    public final static int DEFAULT_MAX_DB_CONN_POOL_SIZE = DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE + DEFAULT_VERTX_WORKER_POOL_SIZE;

    public final static int DEFAULT_IDLE_CONNECTION_TEST_PEROID = 60;

    public final static boolean DEFAULT_TEST_CONNECTION_ON_CHECKIN = true;

    public final static String DT_PROVIDER_CLASS = "com.dtstack.flink.sql.side.rdb.provider.DTC3P0DataSourceProvider";

    public final static String PREFERRED_TEST_QUERY_SQL = "select 1 from dual";

    private transient SQLClient rdbSQLClient;

    public RdbAsyncReqRow(SideInfo sideInfo) {
        super(sideInfo);
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {

        JsonArray inputParams = new JsonArray();
        for (Integer conValIndex : sideInfo.getEqualValIndex()) {
            Object equalObj = input.getField(conValIndex);
            if (equalObj == null) {
                dealMissKey(input, resultFuture);
                return;
            }
            inputParams.add(equalObj);
        }

        String key = buildCacheKey(inputParams);
        if (openCache()) {
            CacheObj val = getFromCache(key);
            if (val != null) {

                if (ECacheContentType.MissVal == val.getType()) {
                    dealMissKey(input, resultFuture);
                    return;
                } else if (ECacheContentType.MultiLine == val.getType()) {
                    List<Row> rowList = Lists.newArrayList();
                    for (Object jsonArray : (List) val.getContent()) {
                        Row row = fillData(input, jsonArray);
                        rowList.add(row);
                    }
                    resultFuture.complete(rowList);
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

                int resultSize = rs.result().getResults().size();
                if (resultSize > 0) {
                    List<Row> rowList = Lists.newArrayList();

                    for (JsonArray line : rs.result().getResults()) {
                        Row row = fillData(input, line);
                        if (openCache()) {
                            cacheContent.add(line);
                        }
                        rowList.add(row);
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
        });
    }

    @Override
    public Row fillData(Row input, Object line) {
        JsonArray jsonArray = (JsonArray) line;
        Row row = new Row(sideInfo.getOutFieldInfoList().size());
        String[] fields = sideInfo.getSideTableInfo().getFieldTypes();
        for (Map.Entry<Integer, Integer> entry : sideInfo.getInFieldIndex().entrySet()) {
            Object obj = input.getField(entry.getValue());
            obj = convertTimeIndictorTypeInfo(entry.getValue(), obj);
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
        if (rdbSQLClient != null) {
            rdbSQLClient.close();
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

    public void setRdbSQLClient(SQLClient rdbSQLClient) {
        this.rdbSQLClient = rdbSQLClient;
    }
}
