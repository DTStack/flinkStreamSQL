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

 

package com.dtstack.flink.sql.side.mongo;

import com.dtstack.flink.sql.enums.ECacheContentType;
import com.dtstack.flink.sql.side.AsyncReqRow;
import com.dtstack.flink.sql.side.CacheMissVal;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.SideTableInfo;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.mongo.MongoAsyncSideInfo;
import com.dtstack.flink.sql.side.mongo.table.MongoSideTableInfo;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Mongo dim table
 * Date: 2018/7/27
 * Company: www.dtstack.com
 * @author xuchao
 */

public class MongoAsyncReqRow extends AsyncReqRow {

    private static final long serialVersionUID = 2098635244857937717L;

    private static final Logger LOG = LoggerFactory.getLogger(MongoAsyncReqRow.class);

    private transient SQLClient MongoClient;

    private final static String Mongo_DRIVER = "com.Mongo.jdbc.Driver";

    private final static int DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE = 10;

    private final static int DEFAULT_VERTX_WORKER_POOL_SIZE = 20;

    private final static int DEFAULT_MAX_DB_CONN_POOL_SIZE = 20;


    public MongoAsyncReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(new MongoAsyncSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        JsonObject MongoClientConfig = new JsonObject();
        MongoSideTableInfo MongoSideTableInfo = (MongoSideTableInfo) sideInfo.getSideTableInfo();
        MongoClientConfig.put("url", MongoSideTableInfo.getUrl())
                .put("driver_class", Mongo_DRIVER)
                .put("max_pool_size", DEFAULT_MAX_DB_CONN_POOL_SIZE)
                .put("user", MongoSideTableInfo.getUserName())
                .put("password", MongoSideTableInfo.getPassword());

        VertxOptions vo = new VertxOptions();
        vo.setEventLoopPoolSize(DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE);
        vo.setWorkerPoolSize(DEFAULT_VERTX_WORKER_POOL_SIZE);
        Vertx vertx = Vertx.vertx(vo);
        MongoClient = JDBCClient.createNonShared(vertx, MongoClientConfig);
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {

        JsonArray inputParams = new JsonArray();
        for(Integer conValIndex : sideInfo.getEqualValIndex()){
            Object equalObj = input.getField(conValIndex);
            if(equalObj == null){
                resultFuture.complete(null);
            }

            inputParams.add(equalObj);
        }

        String key = buildCacheKey(inputParams);
        if(openCache()){
            CacheObj val = getFromCache(key);
            if(val != null){

                if(ECacheContentType.MissVal == val.getType()){
                    dealMissKey(input, resultFuture);
                    return;
                }else if(ECacheContentType.MultiLine == val.getType()){

                    for(Object jsonArray : (List)val.getContent()){
                        Row row = fillData(input, jsonArray);
                        resultFuture.complete(Collections.singleton(row));
                    }

                }else{
                    throw new RuntimeException("not support cache obj type " + val.getType());
                }
                return;
            }
        }

        MongoClient.getConnection(conn -> {
            if (conn.failed()) {
                //Treatment failures
                resultFuture.completeExceptionally(conn.cause());
                return;
            }

            final SQLConnection connection = conn.result();
            String sqlCondition = sideInfo.getSqlCondition();
            connection.queryWithParams(sqlCondition, inputParams, rs -> {
                if (rs.failed()) {
                    LOG.error("Cannot retrieve the data from the database");
                    LOG.error("", rs.cause());
                    resultFuture.complete(null);
                    return;
                }

                List<JsonArray> cacheContent = Lists.newArrayList();

                int resultSize = rs.result().getResults().size();
                if(resultSize > 0){
                    for (JsonArray line : rs.result().getResults()) {
                        Row row = fillData(input, line);
                        if(openCache()){
                            cacheContent.add(line);
                        }
                        resultFuture.complete(Collections.singleton(row));
                    }

                    if(openCache()){
                        putCache(key, CacheObj.buildCacheObj(ECacheContentType.MultiLine, cacheContent));
                    }
                }else{
                    dealMissKey(input, resultFuture);
                    if(openCache()){
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
    public Row fillData(Row input, Object line){
        JsonArray jsonArray = (JsonArray) line;
        Row row = new Row(sideInfo.getOutFieldInfoList().size());
        for(Map.Entry<Integer, Integer> entry : sideInfo.getInFieldIndex().entrySet()){
            Object obj = input.getField(entry.getValue());
            boolean isTimeIndicatorTypeInfo = TimeIndicatorTypeInfo.class.isAssignableFrom(sideInfo.getRowTypeInfo().getTypeAt(entry.getValue()).getClass());

            if(obj instanceof Timestamp && isTimeIndicatorTypeInfo){
                obj = ((Timestamp)obj).getTime();
            }

            row.setField(entry.getKey(), obj);
        }

        for(Map.Entry<Integer, Integer> entry : sideInfo.getSideFieldIndex().entrySet()){
            if(jsonArray == null){
                row.setField(entry.getKey(), null);
            }else{
                row.setField(entry.getKey(), jsonArray.getValue(entry.getValue()));
            }
        }

        return row;
    }

    @Override
    public void close() throws Exception {
        super.close();
        MongoClient.close();
    }

    public String buildCacheKey(JsonArray jsonArray){
        StringBuilder sb = new StringBuilder();
        for(Object ele : jsonArray.getList()){
            sb.append(ele.toString())
                    .append("_");
        }

        return sb.toString();
    }

}
