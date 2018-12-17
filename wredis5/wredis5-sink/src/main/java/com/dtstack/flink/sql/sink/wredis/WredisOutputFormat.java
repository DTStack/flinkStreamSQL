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

package com.dtstack.flink.sql.sink.wredis;

import com.bj58.spat.wredis.client.builder.ClientBuilder;
import com.bj58.spat.wredis.client.config.ClientConfig;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import com.bj58.spat.wredis.client.wjedis.WJedis;
import com.bj58.spat.wredis.client.wjedis.WJedisSentinelPool;
import java.io.IOException;
import java.util.*;

public class WredisOutputFormat extends RichOutputFormat<Tuple2> {

    private String url;

    private String key;

    private String tableName;

    private String password;

    protected String[] fieldNames;

    protected TypeInformation<?>[] fieldTypes;

    protected List<String> primaryKeys;

    protected int timeout;

    WJedis jedis = null;

    public static WJedisSentinelPool redisSentinelPool = null;

    private final String WREDIS_DEMO_NAME =  "wreidsDemoName";

    private WredisOutputFormat(){
    }
    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        establishConnection();
    }

    private void establishConnection() {
        ClientConfig.setConfigCenterAddr(url);
        ClientConfig.init("wredis_client.xml");

        ClientConfig clientConfig = ClientConfig.getClientConfigMap().get(WREDIS_DEMO_NAME);
        clientConfig.setClientName(tableName);
        clientConfig.setAppKey(key);
        clientConfig.setConnectionTimeout(timeout);

        ClientConfig.getClientConfigMap().put(tableName,clientConfig);
        redisSentinelPool =  ClientBuilder.redisSentinel(tableName).build();
        jedis = redisSentinelPool.getResource();
    }

    @Override
    public void writeRecord(Tuple2 record) throws IOException {
        Tuple2<Boolean, Row> tupleTrans = record;
        Boolean retract = tupleTrans.getField(0);
        if (!retract) {
            return;
        }
        Row row = tupleTrans.getField(1);
        if (record.getArity() != fieldNames.length) {
            return;
        }

        HashMap<String, Integer> map = new HashMap<>();

        for (String primaryKey : primaryKeys){
            for (int i=0; i<fieldNames.length; i++){
                if (fieldNames[i].equals(primaryKey)){
                    map.put(primaryKey, i);
                }
            }
        }

        List<String> kvList = new LinkedList<>();
        for (String primaryKey : primaryKeys){
            StringBuilder primaryKV = new StringBuilder();
            int index = map.get(primaryKey).intValue();
            primaryKV.append(primaryKey).append(":").append(row.getField(index));
            kvList.add(primaryKV.toString());
        }

        String perKey = String.join(":", kvList);


        for (int i = 0; i < fieldNames.length; i++) {
            StringBuilder key = new StringBuilder();
            key.append(tableName).append(":").append(perKey).append(":").append(fieldNames[i]);
            jedis.set(key.toString(), (String) row.getField(i));
            //System.out.println("key="+key.toString());
            //System.out.println("value:"+jedis.get(key.toString()));
        }
    }

    @Override
    public void close() throws IOException {
        if (redisSentinelPool != null) {
            redisSentinelPool = null;
        }
        if (jedis != null) {
            jedis.close();
        }

    }

    public static RedisOutputFormatBuilder buildRedisOutputFormat(){
        return new RedisOutputFormatBuilder();
    }

    public static class RedisOutputFormatBuilder{
        private final WredisOutputFormat redisOutputFormat;

        protected RedisOutputFormatBuilder(){
            this.redisOutputFormat = new WredisOutputFormat();
        }

        public RedisOutputFormatBuilder setUrl(String url){
            redisOutputFormat.url = url;
            return this;
        }

        public RedisOutputFormatBuilder setKey(String key){
            redisOutputFormat.key = key;
            return this;
        }

        public RedisOutputFormatBuilder setTableName(String tableName){
            redisOutputFormat.tableName = tableName;
            return this;
        }

        public RedisOutputFormatBuilder setPassword(String password){
            redisOutputFormat.password = password;
            return this;
        }

        public RedisOutputFormatBuilder setFieldNames(String[] fieldNames){
            redisOutputFormat.fieldNames = fieldNames;
            return this;
        }

        public RedisOutputFormatBuilder setFieldTypes(TypeInformation<?>[] fieldTypes){
            redisOutputFormat.fieldTypes = fieldTypes;
            return this;
        }

        public RedisOutputFormatBuilder setPrimaryKeys(List<String > primaryKeys){
            redisOutputFormat.primaryKeys = primaryKeys;
            return this;
        }

        public RedisOutputFormatBuilder setTimeout(int timeout){
            redisOutputFormat.timeout = timeout;
            return this;
        }

        public WredisOutputFormat finish(){
            if (redisOutputFormat.url == null){
                throw new IllegalArgumentException("No URL supplied.");
            }

            if (redisOutputFormat.key == null){
                throw new IllegalArgumentException("No key supplied.");
            }

            if (redisOutputFormat.tableName == null){
                throw new IllegalArgumentException("No tablename supplied.");
            }

            return redisOutputFormat;
        }
    }
}
