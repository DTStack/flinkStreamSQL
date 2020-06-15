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

import com.dtstack.flink.sql.enums.ECacheContentType;
import com.dtstack.flink.sql.enums.ECacheType;
import com.dtstack.flink.sql.metric.MetricConstant;
import com.dtstack.flink.sql.side.cache.AbstractSideCache;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.cache.LRUSideCache;
import com.dtstack.flink.sql.util.ReflectionUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.JoinType;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

/**
 * All interfaces inherit naming rules: type + "AsyncReqRow" such as == "MysqlAsyncReqRow
 * only support Left join / inner join(join),not support right join
 * Date: 2018/7/9
 * Company: www.dtstack.com
 * @author xuchao
 */

public abstract class BaseAsyncReqRow extends RichAsyncFunction<Tuple2<Boolean,Row>, Tuple2<Boolean,Row>> implements ISideReqRow {
    private static final Logger LOG = LoggerFactory.getLogger(BaseAsyncReqRow.class);
    private static final long serialVersionUID = 2098635244857937717L;
    private RuntimeContext runtimeContext;
    private static int TIMEOUT_LOG_FLUSH_NUM = 10;
    private int timeOutNum = 0;
    protected BaseSideInfo sideInfo;
    protected transient Counter parseErrorRecords;

    public BaseAsyncReqRow(BaseSideInfo sideInfo){
        this.sideInfo = sideInfo;
    }
    @Override
    public void setRuntimeContext(RuntimeContext runtimeContext) {
        super.setRuntimeContext(runtimeContext);
        this.runtimeContext = runtimeContext;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        initCache();
        initMetric();
        LOG.info("async dim table config info: {} ", sideInfo.getSideTableInfo().toString());
    }

    private void initCache(){
        AbstractSideTableInfo sideTableInfo = sideInfo.getSideTableInfo();
        if(sideTableInfo.getCacheType() == null || ECacheType.NONE.name().equalsIgnoreCase(sideTableInfo.getCacheType())){
            return;
        }

        AbstractSideCache sideCache;
        if(ECacheType.LRU.name().equalsIgnoreCase(sideTableInfo.getCacheType())){
            sideCache = new LRUSideCache(sideTableInfo);
            sideInfo.setSideCache(sideCache);
        }else{
            throw new RuntimeException("not support side cache with type:" + sideTableInfo.getCacheType());
        }

        sideCache.initCache();
    }

    private void initMetric() {
        parseErrorRecords = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_SIDE_PARSE_ERROR_RECORDS);
    }


    protected Object convertTimeIndictorTypeInfo(Integer index, Object obj) {
        boolean isTimeIndicatorTypeInfo = DataTypes.class.isAssignableFrom(sideInfo.getRowTypeInfo().getTypeAt(index).getClass());

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

    protected void dealMissKey(Tuple2<Boolean,Row> input, ResultFuture<Tuple2<Boolean,Row>> resultFuture){
        if(sideInfo.getJoinType() == JoinType.LEFT){
            //Reserved left table data
            try {
                Row row = fillData(input.f1, null);
                resultFuture.complete(Collections.singleton(new Tuple2<>(input.f0, row)));
            } catch (Exception e) {
                dealFillDataError(input, resultFuture, e);
            }
        }else{
            resultFuture.complete(null);
        }
    }

    protected void dealCacheData(String key, CacheObj missKeyObj) {
        if (openCache()) {
            putCache(key, missKeyObj);
        }
    }

    @Override
    public void timeout(Tuple2<Boolean,Row> input, ResultFuture<Tuple2<Boolean,Row>> resultFuture) throws Exception {

        if(timeOutNum % TIMEOUT_LOG_FLUSH_NUM == 0){
            LOG.info("Async function call has timed out. input:{}, timeOutNum:{}",input.toString(), timeOutNum);
        }
        timeOutNum ++;
        if(sideInfo.getJoinType() == JoinType.LEFT){
            resultFuture.complete(null);
            return;
        }
        if(timeOutNum > sideInfo.getSideTableInfo().getAsyncFailMaxNum(Long.MAX_VALUE)){
            resultFuture.completeExceptionally(new Exception("Async function call timedoutNum beyond limit."));
            return;
        }
        resultFuture.complete(null);
    }

    protected void preInvoke(Tuple2<Boolean,Row> input, ResultFuture<Tuple2<Boolean,Row>> resultFuture)
            throws InvocationTargetException, IllegalAccessException {
        registerTimerAndAddToHandler(input, resultFuture);
    }

    @Override
    public void asyncInvoke(Tuple2<Boolean,Row> row, ResultFuture<Tuple2<Boolean,Row>> resultFuture) throws Exception {
        Tuple2<Boolean,Row> input = Tuple2.of(row.f0, Row.copy(row.f1));
        preInvoke(input, resultFuture);
        Map<String, Object> inputParams = parseInputParam(input);
        if(MapUtils.isEmpty(inputParams)){
            dealMissKey(input, resultFuture);
            return;
        }
        if(isUseCache(inputParams)){
            invokeWithCache(inputParams, input, resultFuture);
            return;
        }
        handleAsyncInvoke(inputParams, input, resultFuture);
    }

    private Map<String, Object> parseInputParam(Tuple2<Boolean,Row> input){
        Map<String, Object> inputParams = Maps.newHashMap();
        for (int i = 0; i < sideInfo.getEqualValIndex().size(); i++) {
            Integer conValIndex = sideInfo.getEqualValIndex().get(i);
            Object equalObj = input.f1.getField(conValIndex);
            if(equalObj == null){
                return inputParams;
            }
            String columnName = sideInfo.getEqualFieldList().get(i);
            inputParams.put(columnName, equalObj);
        }
        return inputParams;
    }

    protected boolean isUseCache(Map<String, Object> inputParams){
        return openCache() && getFromCache(buildCacheKey(inputParams)) != null;
    }

    private void invokeWithCache(Map<String, Object> inputParams, Tuple2<Boolean,Row> input, ResultFuture<Tuple2<Boolean,Row>> resultFuture){
        if (openCache()) {
            CacheObj val = getFromCache(buildCacheKey(inputParams));
            if (val != null) {
                if (ECacheContentType.MissVal == val.getType()) {
                    dealMissKey(input, resultFuture);
                    return;
                }else if(ECacheContentType.SingleLine == val.getType()){
                    try {
                        Row row = fillData(input.f1, val.getContent());
                        resultFuture.complete(Collections.singleton(Tuple2.of(input.f0, row)));
                    } catch (Exception e) {
                        dealFillDataError(input, resultFuture, e);
                    }
                } else if (ECacheContentType.MultiLine == val.getType()) {
                    try {
                        List<Tuple2<Boolean,Row>> rowList = Lists.newArrayList();
                        for (Object one : (List) val.getContent()) {
                            Row row = fillData(input.f1, one);
                            rowList.add(Tuple2.of(input.f0, row));
                        }
                        resultFuture.complete(rowList);
                    } catch (Exception e) {
                        dealFillDataError(input, resultFuture, e);
                    }
                } else {
                    resultFuture.completeExceptionally(new RuntimeException("not support cache obj type " + val.getType()));
                }
                return;
            }
        }
    }

    public abstract void handleAsyncInvoke(Map<String, Object>  inputParams, Tuple2<Boolean,Row> input, ResultFuture<Tuple2<Boolean,Row>> resultFuture) throws Exception;

    public abstract String buildCacheKey(Map<String, Object> inputParams);

    private ProcessingTimeService getProcessingTimeService(){
        return ((StreamingRuntimeContext)this.runtimeContext).getProcessingTimeService();
    }

    protected ScheduledFuture<?> registerTimer(Tuple2<Boolean,Row> input, ResultFuture<Tuple2<Boolean,Row>> resultFuture){
        long timeoutTimestamp = sideInfo.getSideTableInfo().getAsyncTimeout() + getProcessingTimeService().getCurrentProcessingTime();
        return getProcessingTimeService().registerTimer(
                timeoutTimestamp,
                timestamp -> timeout(input, resultFuture));
    }

    protected void registerTimerAndAddToHandler(Tuple2<Boolean,Row> input, ResultFuture<Tuple2<Boolean,Row>> resultFuture)
            throws InvocationTargetException, IllegalAccessException {
        ScheduledFuture<?> timeFuture = registerTimer(input, resultFuture);
        // resultFuture 是ResultHandler 的实例
        Method setTimeoutTimer = ReflectionUtils.getDeclaredMethod(resultFuture, "setTimeoutTimer", ScheduledFuture.class);
        setTimeoutTimer.setAccessible(true);
        setTimeoutTimer.invoke(resultFuture, timeFuture);
    }


    protected void dealFillDataError(Tuple2<Boolean,Row> input, ResultFuture<Tuple2<Boolean,Row>> resultFuture, Throwable e) {
        parseErrorRecords.inc();
        if(parseErrorRecords.getCount() > sideInfo.getSideTableInfo().getAsyncFailMaxNum(Long.MAX_VALUE)){
            LOG.info("dealFillDataError", e);
            resultFuture.completeExceptionally(e);
        } else {
            dealMissKey(input, resultFuture);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
