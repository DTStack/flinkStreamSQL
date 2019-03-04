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

import com.bj58.spat.wtable.ScanKV;
import com.bj58.spat.wtable.ScanReply;
import com.bj58.spat.wtable.WtableClient;
import com.bj58.spat.wtable.exception.WtableException;
import com.dtstack.flink.sql.enums.ECacheContentType;
import com.dtstack.flink.sql.side.*;
import com.dtstack.flink.sql.side.cache.AbsSideCache;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.wtable.rowkeydealer.AbsRowKeyModeDealer;
import com.dtstack.flink.sql.side.wtable.table.WtableSideTableInfo;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.hbase.async.HBaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Date: 2018/8/21
 * Company: www.dtstack.com
 * @author xuchao
 */

public class WtableAsyncReqRow extends AsyncReqRow {

    private static final long serialVersionUID = 2098635104857937717L;
    private static final Logger LOG = LoggerFactory.getLogger(WtableAsyncReqRow.class);

    // WTable客户端，对象是线程安全的，可以在多线程中使用
    private WtableClient client;
    private String tmpPath = "/tmp/";
    private String configFile ;
    private int tableId;
    private String[] colNames;
    private static  boolean missKeyPolicyOpen;

    public WtableAsyncReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(new WtableAsyncSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
        WtableSideTableInfo wtableSideTableInfo = (WtableSideTableInfo) sideTableInfo;
        String nameCenter = wtableSideTableInfo.getNameCenter();
        String bid = wtableSideTableInfo.getBid();
        String password = wtableSideTableInfo.getPassword();
        tableId = wtableSideTableInfo.getTableId();
        colNames = wtableSideTableInfo.getFields();
        missKeyPolicyOpen = wtableSideTableInfo.isMissKeyPolicyOpen();

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
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 根据配置文件初始化WTable客户端
        client = WtableClient.getInstance(configFile);
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {
        Map<String, Object> refData = com.google.common.collect.Maps.newHashMap();
        for (int i = 0; i < sideInfo.getEqualValIndex().size(); i++) {
            Integer conValIndex = sideInfo.getEqualValIndex().get(i);
            Object equalObj = input.getField(conValIndex);
            if(equalObj == null){
                resultFuture.complete(null);
            }

            refData.put(sideInfo.getEqualFieldList().get(i), equalObj);
        }

        String rowKeyStr = ((WtableAsyncSideInfo)sideInfo).getRowKeyBuilder().getRowKey(refData);

        //get from cache
        if(openCache()){
            CacheObj val = getFromCache(rowKeyStr);
            if(val != null){
                if(missKeyPolicyOpen && ECacheContentType.MissVal == val.getType()){
                    dealMissKey(input, resultFuture);
                    return;
                }else if(ECacheContentType.SingleLine == val.getType()){
                    Row row = fillData(input, val.getContent());
                    resultFuture.complete(Collections.singleton(row));
                }else if(ECacheContentType.MultiLine == val.getType()){
                    for(Object one : (List)val.getContent()){
                        Row row = fillData(input, one);
                        resultFuture.complete(Collections.singleton(row));
                    }
                }
                return;
            }
        }

        //异步获取wtable数据
        Map<String, String> sideMap = scanWtable(tableId,rowKeyStr,sideInfo.getSideCache());
        if(sideMap.keySet().size() > 0){
            Row row = fillData(input, sideMap);
            if(openCache()) {
                sideInfo.getSideCache().putCache(rowKeyStr, CacheObj.buildCacheObj(ECacheContentType.SingleLine, sideMap));
            }
            resultFuture.complete(Collections.singleton(row));
        }else{
            dealMissKey(input, resultFuture);

            if(openCache()){
                sideInfo.getSideCache().putCache(rowKeyStr, CacheMissVal.getMissKeyObj());
            }
        }
    }

    private Map<String, String> scanWtable(int tableId, String rowKey, AbsSideCache tmpCache) {
        Map<String, String> subMap = Maps.newHashMap();
        try {
            ScanReply dr = null;
            while(dr == null || !dr.isEnd()) {
                if(dr == null) {
                    dr = client.scan(tableId, rowKey.getBytes(), true, 100);
                } else {
                    dr = client.scanMore(dr);
                }
                for(int i = 0; i < dr.getKvs().size(); i++) {
                    ScanKV kv = dr.getKvs().get(i);
                    System.out.printf("rowKey=%s, colKey=%s, value=%s, score=%d\n",
                            new String(rowKey),
                            new String(kv.getColKey()),
                            new String(kv.getValue()),
                            kv.getScore());
                    String colKey = new String(kv.getColKey());
                    subMap.put(colKey.toUpperCase(),new String(kv.getValue()));
                }
            }
        } catch (WtableException e) {
            e.printStackTrace();
        }
        return subMap;
    }

    @Override
    protected Row fillData(Row input, Object sideInput){
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
    public void close() throws Exception {
        super.close();
        client = null;
    }
}
