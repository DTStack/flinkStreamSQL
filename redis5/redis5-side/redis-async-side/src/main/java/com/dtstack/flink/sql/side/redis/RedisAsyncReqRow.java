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

import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.BaseAsyncReqRow;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.types.Row;

import com.dtstack.flink.sql.enums.ECacheContentType;
import com.dtstack.flink.sql.side.CacheMissVal;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.redis.table.RedisSideReqRow;
import com.dtstack.flink.sql.side.redis.table.RedisSideTableInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
/**
 * @author yanxi
 */
public class RedisAsyncReqRow extends BaseAsyncReqRow {

    private static final long serialVersionUID = -2079908694523987738L;

    private RedisClient redisClient;

    private StatefulRedisConnection<String, String> connection;

    private RedisClusterClient clusterClient;

    private StatefulRedisClusterConnection<String, String> clusterConnection;

    private RedisKeyAsyncCommands<String, String> async;

    private RedisSideTableInfo redisSideTableInfo;

    private RedisSideReqRow redisSideReqRow;

    public RedisAsyncReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, AbstractSideTableInfo sideTableInfo) {
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
        switch (tableInfo.getRedisType()){
            case 1:
                StringBuilder redisUri = new StringBuilder();
                redisUri.append("redis://").append(password).append(url).append("/").append(database);
                redisClient = RedisClient.create(redisUri.toString());
                connection = redisClient.connect();
                async = connection.async();
                break;
            case 2:
                StringBuilder sentinelUri = new StringBuilder();
                sentinelUri.append("redis-sentinel://").append(password)
                        .append(url).append("/").append(database).append("#").append(redisSideTableInfo.getMasterName());
                redisClient = RedisClient.create(sentinelUri.toString());
                connection = redisClient.connect();
                async = connection.async();
                break;
            case 3:
                StringBuilder clusterUri = new StringBuilder();
                clusterUri.append("redis://").append(password).append(url);
                clusterClient = RedisClusterClient.create(clusterUri.toString());
                clusterConnection = clusterClient.connect();
                async = clusterConnection.async();
            default:
        }
    }

    @Override
    public Row fillData(Row input, Object sideInput) {
        return redisSideReqRow.fillData(input, sideInput);
    }

    @Override
    public void handleAsyncInvoke(Map<String, Object> inputParams, CRow input, ResultFuture<CRow> resultFuture) throws Exception {
        String key = buildCacheKey(inputParams);
        Map<String, String> keyValue = Maps.newHashMap();
        List<String> value = async.keys(key + ":*").get();
        String[] values = value.toArray(new String[value.size()]);
        if (values.length == 0) {
            dealMissKey(input, resultFuture);
        } else {
            RedisFuture<List<KeyValue<String, String>>> future = ((RedisStringAsyncCommands) async).mget(values);
            future.thenAccept(new Consumer<List<KeyValue<String, String>>>() {
                @Override
                public void accept(List<KeyValue<String, String>> keyValues) {
                    if (keyValues.size() != 0) {
                        for (int i = 0; i < keyValues.size(); i++) {
                            String[] splitKeys = StringUtils.split(keyValues.get(i).getKey(), ":");
                            keyValue.put(splitKeys[1], splitKeys[2]);
                            keyValue.put(splitKeys[3], keyValues.get(i).getValue());
                        }
                        try {
                            Row row = fillData(input.row(), keyValue);
                            dealCacheData(key, CacheObj.buildCacheObj(ECacheContentType.MultiLine, keyValue));
                            resultFuture.complete(Collections.singleton(new CRow(row, input.change())));
                        } catch (Exception e) {
                            dealFillDataError(resultFuture, e, input);
                        }
                    } else {
                        dealMissKey(input, resultFuture);
                        dealCacheData(key, CacheMissVal.getMissKeyObj());
                    }
                }
            });
        }
    }

    @Override
    public String buildCacheKey(Map<String, Object> inputParams) {
        String kv = StringUtils.join(inputParams.values(), ":");
        String tableName = redisSideTableInfo.getTableName();
        StringBuilder preKey =  new StringBuilder();
        preKey.append(tableName).append(":").append(kv);
        return preKey.toString();
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
