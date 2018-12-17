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

package com.dtstack.flink.sql.side.wredis;

import com.bj58.spat.wredis.client.builder.ClientBuilder;
import com.bj58.spat.wredis.client.config.ClientConfig;
import com.bj58.spat.wredis.client.wjedis.WJedis;
import com.bj58.spat.wredis.client.wjedis.WJedisSentinelPool;
import com.dtstack.flink.sql.side.*;
import com.dtstack.flink.sql.side.wredis.table.WredisSideTableInfo;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class WredisAllReqRow extends AllReqRow{

    private static final long serialVersionUID = 7578879189085344807L;

    private static final Logger LOG = LoggerFactory.getLogger(WredisAllReqRow.class);

    private static final int CONN_RETRY_NUM = 3;

    private static final int TIMEOUT = 10000;

    private WredisSideTableInfo tableInfo;

    public static WJedisSentinelPool jedisSentinelPool = null;

    private final String WREDIS_DEMO_NAME =  "wreidsDemoName";

    private AtomicReference<Map<String, Map<String, String>>> cacheRef = new AtomicReference<>();

    public WredisAllReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(new WredisAllSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
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
        tableInfo = (WredisSideTableInfo) sideInfo.getSideTableInfo();
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
        for(int i=0; i<sideInfo.getEqualValIndex().size(); i++){
            Object equalObj = row.getField(sideInfo.getEqualValIndex().get(i));
            if(equalObj == null){
                out.collect(null);
            }
            String columnName = sideInfo.getEqualFieldList().get(i);
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
        WJedis jedis = null;

        try {
            for(int i=0; i<CONN_RETRY_NUM; i++){

                try{
                    jedis = getJedis(tableInfo.getUrl(), tableInfo.getTableName(), tableInfo.getKey());
                    break;
                }catch (Exception e){
                    if(i == CONN_RETRY_NUM - 1){
                        throw new RuntimeException("", e);
                    }
                    Thread.sleep(5 * 1000);
                }
            }

            String perKey = tableInfo.getTableName() + "*";
            List<String> keys = scan(jedis,perKey,10000);
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
                List<String> realKeys = scan(jedis,pattern,10000);
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
                jedisSentinelPool = null;
            }
        }
    }

    /**
     * 模糊匹配
     * @param pattern key的正则表达式
     * @param count 每次扫描多少条记录，值越大消耗的时间越短，但会影响redis性能。建议设为一千到一万
     * @return 匹配的key集合
     */
    public static List<String> scan(WJedis jedis,String pattern, int count){
        List<String> list = new ArrayList<String>();
        if (jedis == null){
            return list;
        }

        try{
            String cursor = ScanParams.SCAN_POINTER_START;

            ScanParams scanParams = new ScanParams();
            scanParams.count(count);
            scanParams.match(pattern);

            do {
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                list.addAll(scanResult.getResult());
                cursor = scanResult.getStringCursor();
            }while(!"0".equals(cursor));

            return list;
        }catch(Exception e){

            return list;
        }
    }

    private WJedis getJedis(String url, String tableName, String key){
        ClientConfig.setConfigCenterAddr(url);
        ClientConfig.init("wredis_client.xml");

        ClientConfig clientConfig = ClientConfig.getClientConfigMap().get(WREDIS_DEMO_NAME);
        clientConfig.setClientName(tableName);
        clientConfig.setAppKey(key);
        clientConfig.setConnectionTimeout(TIMEOUT);

        ClientConfig.getClientConfigMap().put(tableName,clientConfig);
        jedisSentinelPool =  ClientBuilder.redisSentinel(tableName).build();
        return jedisSentinelPool.getResource();
    }

    public static class WredisAllSideInfo extends SideInfo {

        private static final long serialVersionUID = 1998703966487857613L;

        public WredisAllSideInfo(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
            super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
        }

        @Override
        public void buildEqualInfo(JoinInfo joinInfo, SideTableInfo sideTableInfo) {
            String sideTableName = joinInfo.getSideTableName();
            SqlNode conditionNode = joinInfo.getCondition();

            List<SqlNode> sqlNodeList = Lists.newArrayList();
            if(conditionNode.getKind() == SqlKind.AND){
                sqlNodeList.addAll(Lists.newArrayList(((SqlBasicCall)conditionNode).getOperands()));
            }else{
                sqlNodeList.add(conditionNode);
            }

            for(SqlNode sqlNode : sqlNodeList){
                dealOneEqualCon(sqlNode, sideTableName);
            }
        }
    }
}
