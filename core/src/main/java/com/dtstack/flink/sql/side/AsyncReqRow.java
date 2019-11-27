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



package com.dtstack.flink.sql.side;

import com.dtstack.flink.sql.enums.ECacheType;
import com.dtstack.flink.sql.side.cache.AbsSideCache;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.cache.LRUSideCache;
import org.apache.calcite.sql.JoinType;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.operators.async.queue.StreamRecordQueueEntry;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeoutException;

/**
 * All interfaces inherit naming rules: type + "AsyncReqRow" such as == "MysqlAsyncReqRow
 * only support Left join / inner join(join),not support right join
 * Date: 2018/7/9
 * Company: www.dtstack.com
 * @author xuchao
 */

public abstract class AsyncReqRow extends RichAsyncFunction<Row, Row> implements ISideReqRow {

    private static final long serialVersionUID = 2098635244857937717L;

    protected SideInfo sideInfo;

    public AsyncReqRow(SideInfo sideInfo){
        this.sideInfo = sideInfo;
    }

    @Override
    public void timeout(Row input, ResultFuture<Row> resultFuture) throws Exception {
        StreamRecordQueueEntry<Row> future = (StreamRecordQueueEntry<Row>)resultFuture;
        try {
            if (null == future.get()) {
                new TimeoutException("Async function call has timed out.");
            }
        } catch (Exception e) {
            throw new Exception(e);
        }
    }

    private void initCache(){
        SideTableInfo sideTableInfo = sideInfo.getSideTableInfo();
        if(sideTableInfo.getCacheType() == null || ECacheType.NONE.name().equalsIgnoreCase(sideTableInfo.getCacheType())){
            return;
        }

        AbsSideCache sideCache;
        if(ECacheType.LRU.name().equalsIgnoreCase(sideTableInfo.getCacheType())){
            sideCache = new LRUSideCache(sideTableInfo);
            sideInfo.setSideCache(sideCache);
        }else{
            throw new RuntimeException("not support side cache with type:" + sideTableInfo.getCacheType());
        }

        sideCache.initCache();
    }


    protected Object convertTimeIndictorTypeInfo(Integer index, Object obj) {
        boolean isTimeIndicatorTypeInfo = TimeIndicatorTypeInfo.class.isAssignableFrom(sideInfo.getRowTypeInfo().getTypeAt(index).getClass());

        //Type information for indicating event or processing time. However, it behaves like a regular SQL timestamp but is serialized as Long.
        if (obj instanceof LocalDateTime && isTimeIndicatorTypeInfo) {
            obj = Timestamp.valueOf(((LocalDateTime) obj));
        }
        return obj;
    }

    protected CacheObj getFromCache(String key){
        return sideInfo.getSideCache().getFromCache(key);
    }

    protected void putCache(String key, CacheObj value){
        sideInfo.getSideCache().putCache(key, value);
    }

    protected boolean openCache(){
        return sideInfo.getSideCache() != null;
    }

    protected void dealMissKey(Row input, ResultFuture<Row> resultFuture){
        if(sideInfo.getJoinType() == JoinType.LEFT){
            //Reserved left table data
            Row row = fillData(input, null);
            resultFuture.complete(Collections.singleton(row));
        }else{
            resultFuture.complete(null);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        initCache();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
