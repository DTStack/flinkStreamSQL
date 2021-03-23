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
import com.dtstack.flink.sql.side.BaseAllReqRow;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.redis.enums.RedisType;
import com.dtstack.flink.sql.side.redis.table.RedisSideReqRow;
import com.dtstack.flink.sql.side.redis.table.RedisSideTableInfo;
import com.dtstack.flink.sql.util.RowDataComplete;
import com.esotericsoftware.minlog.Log;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.JoinType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author yanxi
 */
public class RedisAllReqRow extends BaseAllReqRow {

    private static final long serialVersionUID = 7578879189085344807L;

    private static final Pattern HOST_PORT_PATTERN = Pattern.compile("(?<host>(.*)):(?<port>\\d+)*");

    private static final Logger LOG = LoggerFactory.getLogger(RedisAllReqRow.class);

    private static final int CONN_RETRY_NUM = 3;

    private JedisPool pool;

    private JedisSentinelPool jedisSentinelPool;

    private RedisSideTableInfo tableInfo;

    private final AtomicReference<Map<String, Map<String, String>>> cacheRef = new AtomicReference<>();

    private final RedisSideReqRow redisSideReqRow;

    public RedisAllReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, AbstractSideTableInfo sideTableInfo) {
        super(new RedisAllSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
        this.redisSideReqRow = new RedisSideReqRow(super.sideInfo, (RedisSideTableInfo) sideTableInfo);
    }

    @Override
    public BaseRow fillData(BaseRow input, Object sideInput) {
        return redisSideReqRow.fillData(input, sideInput);
    }

    @Override
    protected void initCache() throws SQLException {
        tableInfo = (RedisSideTableInfo) sideInfo.getSideTableInfo();
        Map<String, Map<String, String>> newCache = Maps.newConcurrentMap();
        cacheRef.set(newCache);
        loadData(newCache);
    }

    @Override
    protected void reloadCache() {
        Map<String, Map<String, String>> newCache = Maps.newConcurrentMap();
        try {
            loadData(newCache);
        } catch (SQLException e) {
            LOG.error("", e);
            throw new RuntimeException(e);
        }

        cacheRef.set(newCache);
        LOG.info("----- Redis all cacheRef reload end:{}", Calendar.getInstance());
    }

    @Override
    public void flatMap(BaseRow input, Collector<BaseRow> out) throws Exception {
        GenericRow genericRow = (GenericRow) input;
        Map<String, Object> inputParams = Maps.newHashMap();
        for (Integer conValIndex : sideInfo.getEqualValIndex()) {
            Object equalObj = genericRow.getField(conValIndex);
            if (equalObj == null) {
                if (sideInfo.getJoinType() == JoinType.LEFT) {
                    BaseRow data = fillData(input, null);
                    RowDataComplete.collectBaseRow(out, data);
                }
                return;
            }
            String columnName = sideInfo.getEqualFieldList().get(conValIndex);
            inputParams.put(columnName, equalObj.toString());
        }
        String key = redisSideReqRow.buildCacheKey(inputParams);

        Map<String, String> cacheMap = cacheRef.get().get(key);

        if (cacheMap == null) {
            if (sideInfo.getJoinType() == JoinType.LEFT) {
                BaseRow data = fillData(input, null);
                RowDataComplete.collectBaseRow(out, data);
            } else {
                return;
            }

            return;
        }

        BaseRow newRow = fillData(input, cacheMap);
        RowDataComplete.collectBaseRow(out, newRow);
    }

    private void loadData(Map<String, Map<String, String>> tmpCache) throws SQLException {
        JedisCommands jedis = null;
        try {
            StringBuilder keyPattern = new StringBuilder(tableInfo.getTableName());
            for (int i = 0; i < tableInfo.getPrimaryKeys().size(); i++) {
                keyPattern.append("_").append("*");
            }
            jedis = getJedisWithRetry(CONN_RETRY_NUM);
            if (null == jedis) {
                throw new RuntimeException("redis all load data error,get jedis commands error!");
            }
            Set<String> keys = getRedisKeys(RedisType.parse(tableInfo.getRedisType()), jedis, keyPattern.toString());
            if (CollectionUtils.isEmpty(keys)) {
                return;
            }
            for (String key : keys) {
                tmpCache.put(key, jedis.hgetAll(key));
            }
        } finally {
            if (jedis != null) {
                try {
                    ((Closeable) jedis).close();
                } catch (IOException e) {
                    Log.error("", e);
                }
            }
            if (jedisSentinelPool != null) {
                jedisSentinelPool.close();
            }
            if (pool != null) {
                pool.close();
            }
        }
    }

    private JedisCommands getJedis(RedisSideTableInfo tableInfo) {
        String url = tableInfo.getUrl();
        String password = tableInfo.getPassword();
        String database = tableInfo.getDatabase() == null ? "0" : tableInfo.getDatabase();
        String masterName = tableInfo.getMasterName();
        int timeout = tableInfo.getTimeout();
        if (timeout == 0) {
            timeout = 10000;
        }

        String[] nodes = url.split(",");
        JedisCommands jedis = null;
        GenericObjectPoolConfig poolConfig = setPoolConfig(tableInfo.getMaxTotal(), tableInfo.getMaxIdle(), tableInfo.getMinIdle());
        switch (RedisType.parse(tableInfo.getRedisType())) {
            case STANDALONE:
                String firstIp = null;
                String firstPort = null;
                Matcher standalone = HOST_PORT_PATTERN.matcher(nodes[0]);
                if (standalone.find()) {
                    firstIp = standalone.group("host").trim();
                    firstPort = standalone.group("port").trim();
                }
                if (Objects.nonNull(firstIp)) {
                    pool = new JedisPool(poolConfig, firstIp, Integer.parseInt(firstPort), timeout, password, Integer.parseInt(database));
                    jedis = pool.getResource();
                } else {
                    throw new IllegalArgumentException(
                            String.format("redis url error. current url [%s]", nodes[0]));
                }
                break;
            case SENTINEL:
                Set<String> ipPorts = new HashSet<>(Arrays.asList(nodes));
                jedisSentinelPool = new JedisSentinelPool(masterName, ipPorts, poolConfig, timeout, password, Integer.parseInt(database));
                jedis = jedisSentinelPool.getResource();
                break;
            case CLUSTER:
                Set<HostAndPort> addresses = new HashSet<>();
                // 对ipv6 支持
                for (String node : nodes) {
                    Matcher matcher = HOST_PORT_PATTERN.matcher(node);
                    if (matcher.find()) {
                        String host = matcher.group("host").trim();
                        String portStr = matcher.group("port").trim();
                        if (org.apache.commons.lang3.StringUtils.isNotBlank(host) && org.apache.commons.lang3.StringUtils.isNotBlank(portStr)) {
                            // 转化为int格式的端口
                            int port = Integer.parseInt(portStr);
                            addresses.add(new HostAndPort(host, port));
                        }
                    }
                }
                jedis = new JedisCluster(addresses, timeout, timeout, 10, password, poolConfig);
                break;
            default:
                break;
        }

        return jedis;
    }

    private JedisCommands getJedisWithRetry(int retryNum) {
        while (retryNum-- > 0) {
            try {
                return getJedis(tableInfo);
            } catch (Exception e) {
                if (retryNum <= 0) {
                    throw new RuntimeException("getJedisWithRetry error", e);
                }
                try {
                    String jedisInfo = "url:" + tableInfo.getUrl() + ",pwd:" + tableInfo.getPassword() + ",database:" + tableInfo.getDatabase();
                    LOG.warn("get conn fail, wait for 5 sec and try again, connInfo:" + jedisInfo);
                    Thread.sleep(LOAD_DATA_ERROR_SLEEP_TIME);
                } catch (InterruptedException e1) {
                    LOG.error("", e1);
                }
            }
        }
        return null;
    }

    private Set<String> getRedisKeys(RedisType redisType, JedisCommands jedis, String keyPattern) {
        if (!redisType.equals(RedisType.CLUSTER)) {
            return ((Jedis) jedis).keys(keyPattern);
        }
        Set<String> keys = new TreeSet<>();
        Map<String, JedisPool> clusterNodes = ((JedisCluster) jedis).getClusterNodes();
        for (String k : clusterNodes.keySet()) {
            JedisPool jp = clusterNodes.get(k);
            try (Jedis connection = jp.getResource()) {
                keys.addAll(connection.keys(keyPattern));
            } catch (Exception e) {
                LOG.error("Getting keys error", e);
            }
        }
        return keys;
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
}
