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



package com.dtstack.flink.sql.side.wtable;

import com.bj58.spat.wtable.*;
import com.bj58.spat.wtable.exception.WtableException;
import com.dtstack.flink.sql.side.*;
import com.dtstack.flink.sql.side.wtable.table.WtableSideTableInfo;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class WtableAllReqRow extends AllReqRow {
    private static final Logger LOG = LoggerFactory.getLogger(WtableAllReqRow.class);
    // WTable客户端，对象是线程安全的，可以在多线程中使用
    private WtableClient client;
    private String tmpPath = "/tmp/";

    private String configFile ;
    private AtomicReference<Map<String, Map<String, Object>>> cacheRef = new AtomicReference<>();

    public WtableAllReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(new WtableAllSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
        WtableSideTableInfo wtableSideTableInfo = (WtableSideTableInfo) sideTableInfo;
        String nameCenter = wtableSideTableInfo.getNameCenter();
        String bid = wtableSideTableInfo.getBid();
        String password = wtableSideTableInfo.getPassword();
        StringBuffer sb = new StringBuffer();
        sb.append("namecenter=").append(nameCenter).append("\n");
        sb.append("bid=").append(bid).append("\n");
        sb.append("password=").append(password);

        configFile = tmpPath+ UUID.randomUUID();
        File file = null;
        FileWriter fw;
        BufferedWriter bw = null;
        try {
            file = new File(configFile);
            if (!file.exists()) {
                file.createNewFile();
            }
            fw = new FileWriter(file);
            bw = new BufferedWriter(fw);
            bw.write(sb.toString());

            if (bw != null) {
                bw.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            file.deleteOnExit();
        }
    }

    @Override
    protected Row fillData(Row input, Object sideInput) {
        Map<String, Object> sideInputList = (Map<String, Object>) sideInput;
        Row row = new Row(sideInfo.getOutFieldInfoList().size());
        for(Map.Entry<Integer, Integer> entry : sideInfo.getInFieldIndex().entrySet()){
            Object obj = input.getField(entry.getValue());
            boolean isTimeIndicatorTypeInfo = TimeIndicatorTypeInfo.class.isAssignableFrom(sideInfo.getRowTypeInfo().getTypeAt(entry.getValue()).getClass());

            //Type information for indicating event or processing time. However, it behaves like a regular SQL timestamp but is serialized as Long.
            if(obj instanceof Timestamp && isTimeIndicatorTypeInfo){
                obj = ((Timestamp)obj).getTime();
            }
            row.setField(entry.getKey(), obj);
        }

        for(Map.Entry<Integer, Integer> entry : sideInfo.getSideFieldIndex().entrySet()){
            if(sideInputList == null){
                row.setField(entry.getKey(), null);
            }else{
                String key = sideInfo.getSideFieldNameIndex().get(entry.getKey());
                row.setField(entry.getKey(), sideInputList.get(key));
            }
        }

        return row;
    }

    @Override
    protected void initCache() throws SQLException {
        try {
            // 根据配置文件初始化WTable客户端
            client = WtableClient.getInstance(configFile);
        } catch (WtableException e) {
            e.printStackTrace();
        }
        Map<String, Map<String, Object>> newCache = Maps.newConcurrentMap();
        cacheRef.set(newCache);
        loadData(newCache);
    }

    @Override
    protected void reloadCache() {
        Map<String, Map<String, Object>> newCache = Maps.newConcurrentMap();
        try {
            loadData(newCache);
        } catch (SQLException e) {
            LOG.error("", e);
        }

        cacheRef.set(newCache);
        LOG.info("----- Wtable all cacheRef reload end:{}", Calendar.getInstance());
    }

    @Override
    public void flatMap(Row value, Collector<Row> out) throws Exception {
        Map<String, Object> refData = Maps.newHashMap();
        for (int i = 0; i < sideInfo.getEqualValIndex().size(); i++) {
            Integer conValIndex = sideInfo.getEqualValIndex().get(i);
            Object equalObj = value.getField(conValIndex);
            if(equalObj == null){
                out.collect(null);
            }
            refData.put(sideInfo.getEqualFieldList().get(i), equalObj);
        }

        String rowKeyStr = ((WtableAllSideInfo)sideInfo).getRowKeyBuilder().getRowKey(refData);

        Map<String, Object> cacheList = null;

        SideTableInfo sideTableInfo = sideInfo.getSideTableInfo();
        WtableSideTableInfo wtableSideTableInfo = (WtableSideTableInfo) sideTableInfo;
        if (wtableSideTableInfo.isPreRowKey())
        {
            for (Map.Entry<String, Map<String, Object>> entry : cacheRef.get().entrySet()){
                if (entry.getKey().startsWith(rowKeyStr))
                {
                    cacheList = cacheRef.get().get(entry.getKey());
                    Row row = fillData(value, cacheList);
                    out.collect(row);
                }
            }
        } else {
            cacheList = cacheRef.get().get(rowKeyStr);
            Row row = fillData(value, cacheList);
            out.collect(row);
        }

    }

    private void loadData(Map<String, Map<String, Object>> tmpCache) throws SQLException {
        SideTableInfo sideTableInfo = sideInfo.getSideTableInfo();
        WtableSideTableInfo wtableSideTableInfo = (WtableSideTableInfo) sideTableInfo;
        //全量加载wtable维表数据
        int tableId = wtableSideTableInfo.getTableId();

        dumpWtable(tableId,tmpCache);
    }

    // dump接口可以把一个table或者一个DB的数据分批下载下来
    private void dumpWtable(int tableId,Map<String, Map<String, Object>> tmpCache) {
        try {
            DumpReply dr = null;
            while(dr == null || !dr.isEnd()) {
                if(dr == null) {
                    dr = client.dumpTable(tableId); // dump表id=1
                    //dr = client.dumpDB();   // dump整个DB，包含这个DB下所有表的数据
                } else {
                    // 如果还没拉取完毕，继续拉取
                    dr = client.dumpMore(dr);
                }

                // colSpace=0表示是默认空间的数据，数据是通过set/mSet这类接口设置的；
                // colSpace=1表示是"Z"排序空间的数据，数据是通过zSet/zmSet这类接口设置的。
                for(int i = 0; i < dr.getKvs().size(); i++) {
                    DumpKV kv = dr.getKvs().get(i);
                    System.out.printf("dumpExample: tableId=%d, colSpace=%d, rowKey=%s, colKey=%s, value=%s, score=%d\n",
                            kv.getTableId(), kv.getColSpace(),
                            new String(kv.getRowKey()),
                            new String(kv.getColKey()),
                            new String(kv.getValue()),
                            kv.getScore());
                    String rowKey = new String(kv.getRowKey());
                    String colKey = new String(kv.getColKey());
                    if (tmpCache.containsKey(rowKey)){
                        tmpCache.get(rowKey).put(colKey.toUpperCase(),new String(kv.getValue()));
                    } else {
                        Map<String, Object> subMap = new HashedMap();
                        subMap.put(colKey.toUpperCase(),new String(kv.getValue()));
                        tmpCache.put(rowKey,subMap);
                    }
                }
            }
        } catch (WtableException e) {
            e.printStackTrace();
        }
    }

    private void testWtable(int tableId,Map<String, Map<String, Object>> tmpCache) {
        try {
            ScanReply dr = null;
            while(dr == null || !dr.isEnd()) {
                if(dr == null) {
                    dr = client.scan(tableId, "source12".getBytes(), true, 100);
                } else {
                    dr = client.scanMore(dr);
                }
                for(int i = 0; i < dr.getKvs().size(); i++) {
                    ScanKV kv = dr.getKvs().get(i);
                    System.out.printf("rowKey=%s, colKey=%s, value=%s, score=%d\n",
                            new String("source12"),
                            new String(kv.getColKey()),
                            new String(kv.getValue()),
                            kv.getScore());
                    String rowKey = new String("source12");
                    String colKey = new String(kv.getColKey());
                    if (tmpCache.containsKey(rowKey)){
                        tmpCache.get(rowKey).put(colKey.toUpperCase(),new String(kv.getValue()));
                    } else {
                        Map<String, Object> subMap = new HashedMap();
                        subMap.put(colKey.toUpperCase(),new String(kv.getValue()));
                        tmpCache.put(rowKey,subMap);
                    }
                }
            }
        } catch (WtableException e) {
            e.printStackTrace();
        }
    }
}