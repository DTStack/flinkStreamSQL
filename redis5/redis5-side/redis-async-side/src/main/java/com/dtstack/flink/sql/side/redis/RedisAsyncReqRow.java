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
import com.dtstack.flink.sql.side.redis.table.RedisSideTableInfo;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
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


    public RedisAsyncReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(new RedisAsyncSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
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
        }
    }

    @Override
    protected Row fillData(Row input, Object sideInput) {
        Map<String, String> keyValue = (Map<String, String>) sideInput;
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
            if(keyValue == null){
                row.setField(entry.getKey(), null);
            }else{
                String key = sideInfo.getSideFieldNameIndex().get(entry.getKey());
                row.setField(entry.getKey(), keyValue.get(key));
            }
        }

        return row;
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {
        List<String> keyData = Lists.newLinkedList();
        for (int i = 0; i < sideInfo.getEqualValIndex().size(); i++) {
            Integer conValIndex = sideInfo.getEqualValIndex().get(i);
            Object equalObj = input.getField(conValIndex);
            if(equalObj == null){
                resultFuture.complete(null);
            }

            keyData.add(sideInfo.getEqualFieldList().get(i));
            keyData.add((String) equalObj);
        }

        String key = buildCacheKey(keyData);

        if(openCache()){
            CacheObj val = getFromCache(key);
            if(val != null){

                if(ECacheContentType.MissVal == val.getType()){
                    dealMissKey(input, resultFuture);
                    return;
                }else if(ECacheContentType.MultiLine == val.getType()){
                    Row row = fillData(input, val.getContent());
                    resultFuture.complete(Collections.singleton(row));
                }else{
                    throw new RuntimeException("not support cache obj type " + val.getType());
                }
                return;
            }
        }

        Map<String, String> keyValue = Maps.newHashMap();
        List<String> value = async.keys(key + ":*").get();
        String[] values = value.toArray(new String[value.size()]);
        RedisFuture<List<KeyValue<String, String>>> future =  ((RedisStringAsyncCommands) async).mget(values);
        future.thenAccept(new Consumer<List<KeyValue<String, String>>>() {
            @Override
            public void accept(List<KeyValue<String, String>> keyValues) {
                if (keyValues.size() != 0){
                    for (int i=0; i<keyValues.size(); i++){
                        String[] splitKeys = keyValues.get(i).getKey().split(":");
                        keyValue.put(splitKeys[1], splitKeys[2]);
                        keyValue.put(splitKeys[3], keyValues.get(i).getValue());
                    }
                    Row row = fillData(input, keyValue);
                    resultFuture.complete(Collections.singleton(row));
                    if(openCache()){
                        putCache(key, CacheObj.buildCacheObj(ECacheContentType.MultiLine, keyValue));
                    }
                } else {
                    dealMissKey(input, resultFuture);
                    if(openCache()){
                        putCache(key, CacheMissVal.getMissKeyObj());
                    }
                }
            }
        });
    }

    private String buildCacheKey(List<String> keyData) {
        String kv = String.join(":", keyData);
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
