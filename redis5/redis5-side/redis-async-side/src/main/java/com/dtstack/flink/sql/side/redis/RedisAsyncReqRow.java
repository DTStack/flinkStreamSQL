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
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
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

public class RedisAsyncReqRow extends AsyncReqRow {

    private static final long serialVersionUID = -2079908694523987738L;

    private RedisClient redisClient;

    private StatefulRedisConnection<String, String> connection;

    private RedisAsyncCommands<String, String> async;

    private RedisSideTableInfo redisSideTableInfo;


    public RedisAsyncReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(new RedisAsyncSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        redisSideTableInfo = (RedisSideTableInfo) sideInfo.getSideTableInfo();
        StringBuilder uri = new StringBuilder();
        // TODO: 2018/11/13 根据redis模式，拼接uri
        redisClient = RedisClient.create(uri.toString());
        connection = redisClient.connect();
        async = connection.async();
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

        // TODO: 2018/11/13 插入维表数据
        for(Map.Entry<Integer, Integer> entry : sideInfo.getSideFieldIndex().entrySet()){
            if(keyValue == null){
                row.setField(entry.getKey(), null);
            }else{
                row.setField(entry.getKey(), keyValue.get(entry.getValue()));
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

        // TODO: 2018/11/13 异步实现并缓存
        Map<String, String> keyValue = Maps.newConcurrentMap();
        List<String> keyList = async.keys(key).get();
        for (String aKey : keyList){
            keyValue.put(aKey, async.get(aKey).get());
        }
        Row row = fillData(input, keyValue);
        resultFuture.complete(Collections.singleton(row));

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
        connection.close();
        redisClient.shutdown();
    }

}
