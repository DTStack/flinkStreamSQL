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

import com.dtstack.flink.sql.side.*;
import com.dtstack.flink.sql.side.redis.table.RedisSideReqRow;
import com.dtstack.flink.sql.side.redis.table.RedisSideTableInfo;
import com.esotericsoftware.minlog.Log;
import org.apache.calcite.sql.JoinType;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import com.google.common.collect.Maps;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class RedisAllReqRow extends AllReqRow{

    private static final long serialVersionUID = 7578879189085344807L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisAllReqRow.class);

    private static final int CONN_RETRY_NUM = 3;

    private JedisPool pool;

    private JedisSentinelPool jedisSentinelPool;

    private RedisSideTableInfo tableInfo;

    private AtomicReference<Map<String, Map<String, String>>> cacheRef = new AtomicReference<>();

    private RedisSideReqRow redisSideReqRow;

    public RedisAllReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(new RedisAllSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
        this.redisSideReqRow = new RedisSideReqRow(super.sideInfo);
    }

    @Override
    public Row fillData(Row input, Object sideInput) {
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
        }

        cacheRef.set(newCache);
        LOG.info("----- Redis all cacheRef reload end:{}", Calendar.getInstance());
    }

    @Override
    public void flatMap(Row row, Collector<Row> out) throws Exception {
        Map<String, String> inputParams = Maps.newHashMap();
        for(Integer conValIndex : sideInfo.getEqualValIndex()){
            Object equalObj = row.getField(conValIndex);
            if(equalObj == null){
                if(sideInfo.getJoinType() == JoinType.LEFT){
                    Row data = fillData(row, null);
                    out.collect(data);
                }
                return;
            }
            String columnName = sideInfo.getEqualFieldList().get(conValIndex);
            inputParams.put(columnName, equalObj.toString());
        }
        String key = buildKey(inputParams);

        Map<String, String> cacheMap = cacheRef.get().get(key);

        if (cacheMap == null){
            if(sideInfo.getJoinType() == JoinType.LEFT){
                Row data = fillData(row, null);
                out.collect(data);
            }else{
                return;
            }

            return;
        }

        Row newRow = fillData(row, cacheMap);
        out.collect(newRow);
    }

    private String buildKey(Map<String, String> inputParams) {
        String tableName = tableInfo.getTableName();
        StringBuilder key = new StringBuilder();
        for (int i=0; i<inputParams.size(); i++){
            key.append(tableName).append(":").append(inputParams.keySet().toArray()[i]).append(":")
                    .append(inputParams.get(inputParams.keySet().toArray()[i]));
        }
        return key.toString();
    }

    private void loadData(Map<String, Map<String, String>> tmpCache) throws SQLException {
        JedisCommands jedis = null;

        try {
            for(int i=0; i<CONN_RETRY_NUM; i++){

                try{
                    jedis = getJedis(tableInfo);
                    break;
                }catch (Exception e){
                    if(i == CONN_RETRY_NUM - 1){
                        throw new RuntimeException("", e);
                    }

                    try {
                        String jedisInfo = "url:" + tableInfo.getUrl() + ",pwd:" + tableInfo.getPassword() + ",database:" + tableInfo.getDatabase();
                        LOG.warn("get conn fail, wait for 5 sec and try again, connInfo:" + jedisInfo);
                        Thread.sleep(5 * 1000);
                    } catch (InterruptedException e1) {
                        LOG.error("", e1);
                    }
                }
            }

            if (tableInfo.getRedisType() != 3){
                String perKey = tableInfo.getTableName() + "*";
                Set<String> keys = ((Jedis) jedis).keys(perKey);
                List<String> newPerKeys = new LinkedList<>();
                for (String key : keys){
                    String[] splitKey = key.split(":");
                    String newKey = splitKey[0] + ":" + splitKey[1] + ":" + splitKey[2];
                    newPerKeys.add(newKey);
                }
                List<String> list = newPerKeys.stream().distinct().collect(Collectors.toList());
                for(String key : list){
                    Map<String, String> kv = Maps.newHashMap();
                    String[] primaryKv = key.split(":");
                    kv.put(primaryKv[1], primaryKv[2]);
                    String pattern = key + "*";
                    Set<String> realKeys = ((Jedis) jedis).keys(pattern);
                    for (String realKey : realKeys){
                        kv.put(realKey.split(":")[3], jedis.get(realKey));
                    }
                    tmpCache.put(key, kv);
                }
            } else {
                String perKey = tableInfo.getTableName() + "*";
                Set<String> keys = keys((JedisCluster) jedis, perKey);
                List<String> newPerKeys = new LinkedList<>();
                for (String key : keys){
                    String[] splitKey = key.split(":");
                    String newKey = splitKey[0] + ":" + splitKey[1] + ":" + splitKey[2];
                    newPerKeys.add(newKey);
                }
                List<String> list = newPerKeys.stream().distinct().collect(Collectors.toList());
                for(String key : list){
                    Map<String, String> kv = Maps.newHashMap();
                    String[] primaryKv = key.split(":");
                    kv.put(primaryKv[1], primaryKv[2]);
                    String pattern = key + "*";
                    Set<String> realKeys = keys((JedisCluster) jedis, pattern);
                    for (String realKey : realKeys){
                        kv.put(realKey.split(":")[3], jedis.get(realKey));
                    }
                    tmpCache.put(key, kv);
                }
            }


        } catch (Exception e){
            LOG.error("", e);
        } finally {
            if (jedis != null){
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
        String database = tableInfo.getDatabase();
        int timeout = tableInfo.getTimeout();
        if (timeout == 0){
            timeout = 1000;
        }

        String[] nodes = url.split(",");
        String[] firstIpPort = nodes[0].split(":");
        String firstIp = firstIpPort[0];
        String firstPort = firstIpPort[1];
        Set<HostAndPort> addresses = new HashSet<>();
        Set<String> ipPorts = new HashSet<>();
        for (String ipPort : nodes) {
            ipPorts.add(ipPort);
            String[] ipPortPair = ipPort.split(":");
            addresses.add(new HostAndPort(ipPortPair[0].trim(), Integer.valueOf(ipPortPair[1].trim())));
        }
        if (timeout == 0){
            timeout = 1000;
        }
        JedisCommands jedis = null;
        GenericObjectPoolConfig poolConfig = setPoolConfig(tableInfo.getMaxTotal(), tableInfo.getMaxIdle(), tableInfo.getMinIdle());
        switch (tableInfo.getRedisType()){
            //单机
            case 1:
                pool = new JedisPool(poolConfig, firstIp, Integer.parseInt(firstPort), timeout, password, Integer.parseInt(database));
                jedis = pool.getResource();
                break;
            //哨兵
            case 2:
                jedisSentinelPool = new JedisSentinelPool(tableInfo.getMasterName(), ipPorts, poolConfig, timeout, password, Integer.parseInt(database));
                jedis = jedisSentinelPool.getResource();
                break;
            //集群
            case 3:
                jedis = new JedisCluster(addresses, timeout, timeout,1, poolConfig);
        }

        return jedis;
    }

    private Set<String> keys(JedisCluster jedisCluster, String pattern){
        Set<String> keys = new TreeSet<>();
        Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();
        for(String k : clusterNodes.keySet()){
            JedisPool jp = clusterNodes.get(k);
            Jedis connection = jp.getResource();
            try {
                keys.addAll(connection.keys(pattern));
            } catch (Exception e){
                LOG.error("Getting keys error: {}", e);
            } finally {
                connection.close();
            }
        }
        return keys;
    }

    private GenericObjectPoolConfig setPoolConfig(String maxTotal, String maxIdle, String minIdle){
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        if (maxTotal != null){
            config.setMaxTotal(Integer.parseInt(maxTotal));
        }
        if (maxIdle != null){
            config.setMaxIdle(Integer.parseInt(maxIdle));
        }
        if (minIdle != null){
            config.setMinIdle(Integer.parseInt(minIdle));
        }
        return config;
    }
}
