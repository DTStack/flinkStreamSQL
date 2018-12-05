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
import com.dtstack.flink.sql.side.redis.table.RedisSideTableInfo;
import org.apache.calcite.sql.JoinType;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class RedisAllReqRow extends AllReqRow{

    private static final long serialVersionUID = 7578879189085344807L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisAllReqRow.class);

    private static final int CONN_RETRY_NUM = 3;

    private static final int TIMEOUT = 10000;

    private JedisPool pool;

    private JedisSentinelPool jedisSentinelPool;

    private RedisSideTableInfo tableInfo;

    private AtomicReference<Map<String, Map<String, String>>> cacheRef = new AtomicReference<>();

    public RedisAllReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(new RedisAllSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
    }

    @Override
    protected Row fillData(Row input, Object sideInput) {
        Map<String, String> sideInputMap = (Map<String, String>) sideInput;
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
            if(sideInputMap == null){
                row.setField(entry.getKey(), null);
            }else{
                String key = sideInfo.getSideFieldNameIndex().get(entry.getKey());
                row.setField(entry.getKey(), sideInputMap.get(key));
            }
        }

        return row;
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
                out.collect(null);
            }
            String columnName = sideInfo.getEqualFieldList().get(conValIndex);
            inputParams.put(columnName, (String) equalObj);
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
        Jedis jedis = null;

        try {
            for(int i=0; i<CONN_RETRY_NUM; i++){

                try{
                    jedis = getJedis(tableInfo.getUrl(), tableInfo.getPassword(), tableInfo.getDatabase());
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
                        e1.printStackTrace();
                    }
                }
            }

            String perKey = tableInfo.getTableName() + "*";
            Set<String> keys = jedis.keys(perKey);
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
                Set<String> realKeys = jedis.keys(pattern);
                for (String realKey : realKeys){
                    kv.put(realKey.split(":")[3], jedis.get(realKey));
                }
                tmpCache.put(key, kv);
            }


        } catch (Exception e){
            LOG.error("", e);
        } finally {
            if (jedis != null){
                jedis.close();
            }
            if (jedisSentinelPool != null) {
                jedisSentinelPool.close();
            }
            if (pool != null) {
                pool.close();
            }
        }
    }

    private Jedis getJedis(String url, String password, String database){
        JedisPoolConfig poolConfig = new JedisPoolConfig();
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
            jedisSentinelPool = new JedisSentinelPool("Master", ipPorts, poolConfig, TIMEOUT, password, Integer.parseInt(database));
            return jedisSentinelPool.getResource();
        } else {
            String[] ipPortPair = nodes[0].split(":");
            String ip = ipPortPair[0];
            String port = ipPortPair[1];
            pool = new JedisPool(poolConfig, ip, Integer.parseInt(port), TIMEOUT, password, Integer.parseInt(database));
            return pool.getResource();
        }
    }
}
