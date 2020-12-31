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

import com.dtstack.flink.sql.outputformat.AbstractDtRichOutputFormat;
import com.dtstack.flink.sql.sink.redis.enums.RedisType;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author yanxi
 */
public class RedisOutputFormat extends AbstractDtRichOutputFormat<Tuple2> {
    private static final Logger LOG = LoggerFactory.getLogger(RedisOutputFormat.class);
    protected String[] fieldNames;
    protected TypeInformation<?>[] fieldTypes;
    protected List<String> primaryKeys;
    protected int timeout = 10000;
    private String url;
    private String database = "0";
    private String tableName;
    private String password;
    private int redisType;
    private String maxTotal;
    private String maxIdle;
    private String minIdle;
    private String masterName;
    private JedisPool pool;

    private JedisCommands jedis;

    private JedisSentinelPool jedisSentinelPool;

    private GenericObjectPoolConfig poolConfig;

    private RedisOutputFormat() {
    }

    public static RedisOutputFormatBuilder buildRedisOutputFormat() {
        return new RedisOutputFormatBuilder();
    }

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        establishConnection();
        initMetric();
    }

    private GenericObjectPoolConfig setPoolConfig(String maxTotal, String maxIdle, String minIdle) {
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        if (maxTotal != null) {
            config.setMaxTotal(Integer.parseInt(maxTotal));
        }
        if (maxIdle != null) {
            config.setMaxIdle(Integer.parseInt(maxIdle));
        }
        if (minIdle != null) {
            config.setMinIdle(Integer.parseInt(minIdle));
        }
        return config;
    }

    private void establishConnection() {
        poolConfig = setPoolConfig(maxTotal, maxIdle, minIdle);
        String[] nodes = StringUtils.split(url, ",");
        String[] firstIpPort = StringUtils.split(nodes[0], ":");
        String firstIp = firstIpPort[0];
        String firstPort = firstIpPort[1];
        Set<HostAndPort> addresses = new HashSet<>();
        Set<String> ipPorts = new HashSet<>();
        for (String ipPort : nodes) {
            ipPorts.add(ipPort);
            String[] ipPortPair = StringUtils.split(ipPort, ":");
            addresses.add(new HostAndPort(ipPortPair[0].trim(), Integer.parseInt(ipPortPair[1].trim())));
        }

        switch (RedisType.parse(redisType)) {
            case STANDALONE:
                pool = new JedisPool(poolConfig, firstIp, Integer.parseInt(firstPort), timeout, password, Integer.parseInt(database));
                jedis = pool.getResource();
                break;
            case SENTINEL:
                jedisSentinelPool = new JedisSentinelPool(masterName, ipPorts, poolConfig, timeout, password, Integer.parseInt(database));
                jedis = jedisSentinelPool.getResource();
                break;
            case CLUSTER:
                jedis = new JedisCluster(addresses, timeout, timeout, 10, password, poolConfig);
                break;
            default:
                throw new RuntimeException("unsupported redis type[ " + redisType + "]");
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
        if (row.getArity() != fieldNames.length) {
            return;
        }
        Map<String, Object> refData = Maps.newHashMap();
        for (int i = 0; i < fieldNames.length; i++) {
            refData.put(fieldNames[i], row.getField(i));
        }
        String redisKey = buildCacheKey(refData);
        refData.forEach((key, value) -> jedis.hset(redisKey, key, String.valueOf(value)));

        if (outRecords.getCount() % ROW_PRINT_FREQUENCY == 0) {
            LOG.info(record.toString());
        }
        outRecords.inc();
    }

    @Override
    public void close() throws IOException {
        if (jedisSentinelPool != null) {
            jedisSentinelPool.close();
        }
        if (pool != null) {
            pool.close();
        }
        if (jedis != null) {
            if (jedis instanceof Closeable) {
                ((Closeable) jedis).close();
            }
        }

    }

    public String buildCacheKey(Map<String, Object> refData) {
        StringBuilder keyBuilder = new StringBuilder(tableName);
        for (String primaryKey : primaryKeys) {
            if (!refData.containsKey(primaryKey)) {
                return null;
            }
            keyBuilder.append("_").append(refData.get(primaryKey));
        }
        return keyBuilder.toString();
    }

    public static class RedisOutputFormatBuilder {
        private final RedisOutputFormat redisOutputFormat;

        protected RedisOutputFormatBuilder() {
            this.redisOutputFormat = new RedisOutputFormat();
        }

        public RedisOutputFormatBuilder setUrl(String url) {
            redisOutputFormat.url = url;
            return this;
        }

        public RedisOutputFormatBuilder setDatabase(String database) {
            redisOutputFormat.database = database;
            return this;
        }

        public RedisOutputFormatBuilder setTableName(String tableName) {
            redisOutputFormat.tableName = tableName;
            return this;
        }

        public RedisOutputFormatBuilder setPassword(String password) {
            redisOutputFormat.password = password;
            return this;
        }

        public RedisOutputFormatBuilder setFieldNames(String[] fieldNames) {
            redisOutputFormat.fieldNames = fieldNames;
            return this;
        }

        public RedisOutputFormatBuilder setFieldTypes(TypeInformation<?>[] fieldTypes) {
            redisOutputFormat.fieldTypes = fieldTypes;
            return this;
        }

        public RedisOutputFormatBuilder setPrimaryKeys(List<String> primaryKeys) {
            redisOutputFormat.primaryKeys = primaryKeys;
            return this;
        }

        public RedisOutputFormatBuilder setTimeout(int timeout) {
            redisOutputFormat.timeout = timeout;
            return this;
        }

        public RedisOutputFormatBuilder setRedisType(int redisType) {
            redisOutputFormat.redisType = redisType;
            return this;
        }

        public RedisOutputFormatBuilder setMaxTotal(String maxTotal) {
            redisOutputFormat.maxTotal = maxTotal;
            return this;
        }

        public RedisOutputFormatBuilder setMaxIdle(String maxIdle) {
            redisOutputFormat.maxIdle = maxIdle;
            return this;
        }

        public RedisOutputFormatBuilder setMinIdle(String minIdle) {
            redisOutputFormat.minIdle = minIdle;
            return this;
        }

        public RedisOutputFormatBuilder setMasterName(String masterName) {
            redisOutputFormat.masterName = masterName;
            return this;
        }

        public RedisOutputFormat finish() {
            if (redisOutputFormat.url == null) {
                throw new IllegalArgumentException("No URL supplied.");
            }

            if (redisOutputFormat.tableName == null) {
                throw new IllegalArgumentException("No tablename supplied.");
            }

            return redisOutputFormat;
        }
    }
}
