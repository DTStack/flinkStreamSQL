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

import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.cache.AbstractSideCache;
import com.dtstack.flink.sql.side.table.BaseAsyncTableFunction;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.JoinType;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.hbase.async.HBaseClient;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Reason:
 * Date: 2018/9/10
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public abstract class AbstractRowKeyModeDealer {

    protected Map<String, String> colRefType;

    protected String[] colNames;

    protected HBaseClient hBaseClient;

    protected boolean openCache;

    protected JoinType joinType;

    protected List<FieldInfo> outFieldInfoList;

    protected static final String ROWKEY = "rowkey";

    protected BaseSideInfo sideInfo;

    protected BaseAsyncTableFunction baseAsyncTableFunction;

    //key:Returns the value of the position, returns the index values ​​in the input data
    protected Map<Integer, Integer> inFieldIndex = Maps.newHashMap();

    protected Map<Integer, Integer> sideFieldIndex = Maps.newHashMap();

    public AbstractRowKeyModeDealer(Map<String, String> colRefType
            , String[] colNames
            , HBaseClient hBaseClient
            , boolean openCache
            , BaseSideInfo sideInfo
            , BaseAsyncTableFunction baseAsyncTableFunction) {
        this(colRefType, colNames, hBaseClient, openCache, sideInfo.getJoinType(), sideInfo.getOutFieldInfoList(), sideInfo.getInFieldIndex(), sideInfo.getSideFieldIndex());
        this.sideInfo = sideInfo;
        this.baseAsyncTableFunction = baseAsyncTableFunction;
    }

    public AbstractRowKeyModeDealer(Map<String, String> colRefType
            , String[] colNames
            , HBaseClient hBaseClient
            , boolean openCache
            , JoinType joinType
            , List<FieldInfo> outFieldInfoList
            , Map<Integer, Integer> inFieldIndex
            , Map<Integer, Integer> sideFieldIndex) {
        this.colRefType = colRefType;
        this.colNames = colNames;
        this.hBaseClient = hBaseClient;
        this.openCache = openCache;
        this.joinType = joinType;
        this.outFieldInfoList = outFieldInfoList;
        this.inFieldIndex = inFieldIndex;
        this.sideFieldIndex = sideFieldIndex;
    }

    /**
     * left join未关联上的数据
     *
     * @param input        流数据
     * @param resultFuture 发送到下游
     */
    protected void dealMissKey(Row input, ResultFuture<Row> resultFuture) {
        if (joinType == JoinType.LEFT) {
            try {
                //保留left 表数据
                Row row = fillData(input, null);
                resultFuture.complete(Collections.singleton(row));
            } catch (Exception e) {
                resultFuture.completeExceptionally(e);
            }
        } else {
            resultFuture.complete(Collections.EMPTY_LIST);
        }
    }

    /**
     * 数据填充
     *
     * @param input     流数据
     * @param sideInput 维表数据
     * @return
     */
    protected Row fillData(Row input, Object sideInput) {

        List<Object> sideInputList = (List<Object>) sideInput;
        Row row = new Row(outFieldInfoList.size());
        for (Map.Entry<Integer, Integer> entry : inFieldIndex.entrySet()) {
            Object obj = input.getField(entry.getValue());
            row.setField(entry.getKey(), obj);
        }

        for (Map.Entry<Integer, Integer> entry : sideFieldIndex.entrySet()) {
            if (sideInputList == null) {
                row.setField(entry.getKey(), null);
            } else {
                row.setField(entry.getKey(), sideInputList.get(entry.getValue()));
            }
        }

        return row;
    }

    /**
     * 获取hbase数据
     *
     * @param tableName    表名
     * @param rowKeyStr    rk
     * @param input        流中的数据
     * @param resultFuture 发送到下游
     * @param sideCache    维表缓存
     */
    public abstract void asyncGetData(String tableName, String rowKeyStr, Row input, ResultFuture<Row> resultFuture,
                                      AbstractSideCache sideCache);

    /**
     * 获取hbase数据
     *
     * @param tableName 表名
     * @param rowKeyStr rk
     * @param future    发送到下游
     * @param sideCache 维表缓存
     */
    public abstract void asyncGetData(String tableName, String rowKeyStr, CompletableFuture<Collection<Row>> future, AbstractSideCache sideCache);
}
