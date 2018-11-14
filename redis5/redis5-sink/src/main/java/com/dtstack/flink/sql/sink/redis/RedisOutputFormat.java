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

package com.dtstack.flink.sql.sink.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import redis.clients.jedis.*;

import java.io.IOException;
import java.util.*;

public class RedisOutputFormat extends RichOutputFormat<Tuple2> {

    private String url;

    private String database;

    private String tableName;

    private String password;

    protected String[] fieldNames;

    protected TypeInformation<?>[] fieldTypes;

    protected List<String> primaryKeys;

    protected int timeout;

    private JedisPool pool;

    private Jedis jedis;

    private JedisSentinelPool jedisSentinelPool;

    private GenericObjectPoolConfig poolConfig;

    private RedisOutputFormat(){
    }
    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        establishConnection();
    }

    private void establishConnection() {
        poolConfig = new GenericObjectPoolConfig();
        String[] nodes = url.split(",");
        if (nodes.length > 1){
            //cluster
            Set<HostAndPort> addresses = new HashSet<>();
            Set<String> ipPorts = new HashSet<>();
            for (String ipPort : nodes) {
                ipPorts.add(ipPort);
                String[] ipPortPair = ipPort.split(":");
                addresses.add(new HostAndPort(ipPortPair[0].trim(), Integer.valueOf(ipPortPair[1].trim())));
            }
            jedisSentinelPool = new JedisSentinelPool("Master", ipPorts, poolConfig, timeout, password, Integer.parseInt(database));
            jedis = jedisSentinelPool.getResource();
        } else {
            String[] ipPortPair = nodes[0].split(":");
            String ip = ipPortPair[0];
            String port = ipPortPair[1];
            pool = new JedisPool(poolConfig, ip, Integer.parseInt(port), timeout, password, Integer.parseInt(database));
            jedis = pool.getResource();
        }
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
            jedis.append(key.toString(), (String) row.getField(i));
        }
    }

    @Override
    public void close() throws IOException {
        if (jedisSentinelPool != null) {
            jedisSentinelPool.close();
        }
        if (pool != null) {
            pool.close();
        }

    }

    public static RedisOutputFormatBuilder buildRedisOutputFormat(){
        return new RedisOutputFormatBuilder();
    }

    public static class RedisOutputFormatBuilder{
        private final RedisOutputFormat redisOutputFormat;

        protected RedisOutputFormatBuilder(){
            this.redisOutputFormat = new RedisOutputFormat();
        }

        public RedisOutputFormatBuilder setUrl(String url){
            redisOutputFormat.url = url;
            return this;
        }

        public RedisOutputFormatBuilder setDatabase(String database){
            redisOutputFormat.database = database;
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

        public RedisOutputFormat finish(){
            if (redisOutputFormat.url == null){
                throw new IllegalArgumentException("No URL supplied.");
            }

            if (redisOutputFormat.database == null){
                throw new IllegalArgumentException("No database supplied.");
            }

            if (redisOutputFormat.tableName == null){
                throw new IllegalArgumentException("No tablename supplied.");
            }

            if (redisOutputFormat.password == null){
                throw new IllegalArgumentException("No password supplied.");
            }

            return redisOutputFormat;
        }
    }
}
