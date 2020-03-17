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

package com.dtstack.flink.sql.side.redis;

import com.dtstack.flink.sql.enums.ECacheContentType;
import com.dtstack.flink.sql.side.*;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.redis.enums.RedisType;
import com.dtstack.flink.sql.side.redis.table.RedisSideReqRow;
import com.dtstack.flink.sql.side.redis.table.RedisSideTableInfo;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import com.google.common.collect.Maps;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class RedisAsyncReqRow extends AsyncReqRow {

    private static final long serialVersionUID = -2079908694523987738L;

    private RedisClient redisClient;

    private StatefulRedisConnection<String, String> connection;

    private RedisClusterClient clusterClient;

    private StatefulRedisClusterConnection<String, String> clusterConnection;

    private RedisKeyAsyncCommands<String, String> async;

    private RedisSideTableInfo redisSideTableInfo;

    private RedisSideReqRow redisSideReqRow;

    public RedisAsyncReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(new RedisAsyncSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
        redisSideReqRow = new RedisSideReqRow(super.sideInfo);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        redisSideTableInfo = (RedisSideTableInfo) sideInfo.getSideTableInfo();
        buildRedisClient(redisSideTableInfo);
    }

    private void buildRedisClient(RedisSideTableInfo tableInfo){
        String url = redisSideTableInfo.getUrl();
        String password = redisSideTableInfo.getPassword();
        if (password != null){
            password = password + "@";
        } else {
            password = "";
        }
        String database = redisSideTableInfo.getDatabase();
        if (database == null){
            database = "0";
        }
        switch (RedisType.parse(tableInfo.getRedisType())){
            case STANDALONE:
                StringBuilder redisUri = new StringBuilder();
                redisUri.append("redis://").append(password).append(url).append("/").append(database);
                redisClient = RedisClient.create(redisUri.toString());
                connection = redisClient.connect();
                async = connection.async();
                break;
            case SENTINEL:
                StringBuilder sentinelUri = new StringBuilder();
                sentinelUri.append("redis-sentinel://").append(password)
                        .append(url).append("/").append(database).append("#").append(redisSideTableInfo.getMasterName());
                redisClient = RedisClient.create(sentinelUri.toString());
                connection = redisClient.connect();
                async = connection.async();
                break;
            case CLUSTER:
                StringBuilder clusterUri = new StringBuilder();
                clusterUri.append("redis://").append(password).append(url);
                clusterClient = RedisClusterClient.create(clusterUri.toString());
                clusterConnection = clusterClient.connect();
                async = clusterConnection.async();
            default:
                break;
        }
    }

    @Override
    public Row fillData(Row input, Object sideInput) {
        return redisSideReqRow.fillData(input, sideInput);
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {
        Row inputRow = Row.copy(input);
        Map<String, Object> refData = Maps.newHashMap();
        for (int i = 0; i < sideInfo.getEqualValIndex().size(); i++) {
            Integer conValIndex = sideInfo.getEqualValIndex().get(i);
            Object equalObj = inputRow.getField(conValIndex);
            if(equalObj == null){
                dealMissKey(inputRow, resultFuture);
                return;
            }
            refData.put(sideInfo.getEqualFieldList().get(i), equalObj);
        }

        String key = buildCacheKey(refData);
        if(StringUtils.isBlank(key)){
            return;
        }
        if(openCache()){
            CacheObj val = getFromCache(key);
            if(val != null){
                if(ECacheContentType.MissVal == val.getType()){
                    dealMissKey(inputRow, resultFuture);
                    return;
                }else if(ECacheContentType.MultiLine == val.getType()){
                    try {
                        Row row = fillData(inputRow, val.getContent());
                        resultFuture.complete(Collections.singleton(row));
                    } catch (Exception e) {
                        dealFillDataError(resultFuture, e, inputRow);
                    }
                }else{
                    RuntimeException exception = new RuntimeException("not support cache obj type " + val.getType());
                    resultFuture.completeExceptionally(exception);
                }
                return;
            }
        }

        RedisFuture<Map<String, String>> future = ((RedisHashAsyncCommands) async).hgetall(key);
        future.thenAccept(new Consumer<Map<String, String>>() {
            @Override
            public void accept(Map<String, String> values) {
                if (MapUtils.isNotEmpty(values)) {
                    try {
                        Row row = fillData(inputRow, values);
                        dealCacheData(key,CacheObj.buildCacheObj(ECacheContentType.MultiLine, values));
                        resultFuture.complete(Collections.singleton(row));
                    } catch (Exception e) {
                        dealFillDataError(resultFuture, e, inputRow);
                    }
                } else {
                    dealMissKey(inputRow, resultFuture);
                    dealCacheData(key,CacheMissVal.getMissKeyObj());
                }
            }
        });
    }

    private String buildCacheKey(Map<String, Object> refData) {
        StringBuilder keyBuilder = new StringBuilder(redisSideTableInfo.getTableName());
        List<String> primaryKeys = redisSideTableInfo.getPrimaryKeys();
        for(String primaryKey : primaryKeys){
            if(!refData.containsKey(primaryKey)){
                return null;
            }
            keyBuilder.append("_").append(refData.get(primaryKey));
        }
        return keyBuilder.toString();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null){
            connection.close();
        }
        if (redisClient != null){
            redisClient.shutdown();
        }
        if (clusterConnection != null){
            clusterConnection.close();
        }
        if (clusterClient != null){
            clusterClient.shutdown();
        }
    }

}
