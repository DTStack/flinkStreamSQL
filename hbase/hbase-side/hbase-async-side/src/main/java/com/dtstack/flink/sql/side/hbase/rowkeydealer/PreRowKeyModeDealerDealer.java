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

 

package com.dtstack.flink.sql.side.hbase.rowkeydealer;

import com.dtstack.flink.sql.enums.ECacheContentType;
import com.dtstack.flink.sql.side.CacheMissVal;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.cache.AbstractSideCache;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.hbase.utils.HbaseUtils;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.JoinType;
import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.types.Row;
import org.hbase.async.BinaryPrefixComparator;
import org.hbase.async.Bytes;
import org.hbase.async.CompareFilter;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.RowFilter;
import org.hbase.async.ScanFilter;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Reason:
 * Date: 2018/9/10
 * Company: www.dtstack.com
 * @author xuchao
 */

public class PreRowKeyModeDealerDealer extends AbstractRowKeyModeDealer {

    private static final Logger LOG = LoggerFactory.getLogger(PreRowKeyModeDealerDealer.class);

    public PreRowKeyModeDealerDealer(Map<String, String> colRefType, String[] colNames, HBaseClient hBaseClient,
                                     boolean openCache, JoinType joinType, List<FieldInfo> outFieldInfoList,
                                     Map<Integer, Integer> inFieldIndex, Map<Integer, Integer> sideFieldIndex) {
        super(colRefType, colNames, hBaseClient, openCache, joinType, outFieldInfoList, inFieldIndex, sideFieldIndex);
    }

    @Override
    public void asyncGetData(String tableName, String rowKeyStr, CRow input, ResultFuture<CRow> resultFuture,
                             AbstractSideCache sideCache) {
        Scanner prefixScanner = hBaseClient.newScanner(tableName);
        ScanFilter scanFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.UTF8(rowKeyStr)));
        prefixScanner.setFilter(scanFilter);
        prefixScanner.setMaxVersions(1);

        prefixScanner.nextRows().addCallbacks(arg -> {
            dealOneRow(arg, rowKeyStr, input, resultFuture, sideCache);
            return "";
        }, arg2 -> dealFail(arg2, input, resultFuture));
    }


    private String dealOneRow(ArrayList<ArrayList<KeyValue>> args, String rowKeyStr, CRow input, ResultFuture<CRow> resultFuture, AbstractSideCache sideCache) {
        if(args == null || args.size() == 0){
            dealMissKey(input, resultFuture);
            if (openCache) {
                sideCache.putCache(rowKeyStr, CacheMissVal.getMissKeyObj());
            }
        }

        List<Object> cacheContent = Lists.newArrayList();
        List<CRow> rowList = Lists.newArrayList();

        for(List<KeyValue> oneRow : args){
            try {
                Map<String, Object> sideMap = Maps.newHashMap();
                for (KeyValue keyValue : oneRow) {
                    String cf = new String(keyValue.family());
                    String col = new String(keyValue.qualifier());
                    String mapKey = cf + ":" + col;

                    //The table format defined using different data type conversion byte
                    String colType = colRefType.get(mapKey);
                    Object val = HbaseUtils.convertByte(keyValue.value(), colType);
                    sideMap.put(mapKey, val);
                }
                try {
                    if (oneRow.size() > 0) {
                        //The order of the fields defined in the data conversion table
                        List<Object> sideVal = Lists.newArrayList();
                        for (String key : colNames) {
                            Object val = sideMap.get(key);
                            if (val == null) {
                                System.out.println("can't get data with column " + key);
                                LOG.error("can't get data with column " + key);
                            }

                            sideVal.add(val);
                        }

                        Row row = fillData(input.row(), sideVal);
                        if (openCache) {
                            cacheContent.add(sideVal);
                        }
                        rowList.add(new CRow(row, input.change()));
                    }
                }catch (Exception e) {
                    resultFuture.completeExceptionally(e);
                }
            } catch (Exception e) {
                resultFuture.complete(null);
                LOG.error("record:" + input);
                LOG.error("get side record exception:", e);
            }
        }

        if (rowList.size() > 0){
            resultFuture.complete(rowList);
        }

        if(openCache){
            sideCache.putCache(rowKeyStr, CacheObj.buildCacheObj(ECacheContentType.MultiLine, cacheContent));
        }

        return "";
    }

    private String dealFail(Object arg2, CRow input, ResultFuture<CRow> resultFuture){
        LOG.error("record:" + input);
        LOG.error("get side record exception:" + arg2);
        resultFuture.complete(null);
        return "";
    }
}
