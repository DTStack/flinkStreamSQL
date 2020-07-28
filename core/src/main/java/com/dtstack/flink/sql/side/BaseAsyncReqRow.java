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
import com.dtstack.flink.sql.factory.DTThreadFactory;
import com.dtstack.flink.sql.metric.MetricConstant;
import com.dtstack.flink.sql.side.cache.AbstractSideCache;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.cache.LRUSideCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.JoinType;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.operators.async.queue.StreamRecordQueueEntry;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * All interfaces inherit naming rules: type + "AsyncReqRow" such as == "MysqlAsyncReqRow
 * only support Left join / inner join(join),not support right join
 * Date: 2018/7/9
 * Company: www.dtstack.com
 * @author xuchao
 */

public abstract class BaseAsyncReqRow extends RichAsyncFunction<CRow, CRow> implements ISideReqRow {
    private static final Logger LOG = LoggerFactory.getLogger(BaseAsyncReqRow.class);
    private static final long serialVersionUID = 2098635244857937717L;
    private RuntimeContext runtimeContext;
    private static int TIMEOUT_LOG_FLUSH_NUM = 10;
    private int timeOutNum = 0;
    protected BaseSideInfo sideInfo;
    protected transient Counter parseErrorRecords;
    private transient ThreadPoolExecutor cancelExecutor;

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
        cancelExecutor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(100000),
                new DTThreadFactory("cancel-timer-executor"));
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

    protected CacheObj getFromCache(String key){
        return sideInfo.getSideCache().getFromCache(key);
    }

    protected void putCache(String key, CacheObj value){
        sideInfo.getSideCache().putCache(key, value);
    }

    protected boolean openCache(){
        return sideInfo.getSideCache() != null;
    }

    protected void dealMissKey(CRow input, ResultFuture<CRow> resultFuture){
        if(sideInfo.getJoinType() == JoinType.LEFT){
            //Reserved left table data
            try {
                Row row = fillData(input.row(), null);
                resultFuture.complete(Collections.singleton(new CRow(row, input.change())));
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
    public void timeout(CRow input, ResultFuture<CRow> resultFuture) throws Exception {

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

    protected void preInvoke(CRow input, ResultFuture<CRow> resultFuture){
        ScheduledFuture<?> timeFuture = registerTimer(input, resultFuture);
        cancelTimerWhenComplete(resultFuture, timeFuture);
    }

    @Override
    public void asyncInvoke(CRow row, ResultFuture<CRow> resultFuture) throws Exception {
        CRow input = new CRow(Row.copy(row.row()), row.change());
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

    private Map<String, Object> parseInputParam(CRow input){
        Map<String, Object> inputParams = Maps.newHashMap();
        for (int i = 0; i < sideInfo.getEqualValIndex().size(); i++) {
            Integer conValIndex = sideInfo.getEqualValIndex().get(i);
            Object equalObj = input.row().getField(conValIndex);
            if(equalObj == null){
                return inputParams;
            }
            String columnName = sideInfo.getEqualFieldList().get(i);
            inputParams.put(columnName, equalObj);
        }
        return inputParams;
    }

    private void constantField() {

    }

    protected boolean isUseCache(Map<String, Object> inputParams){
        return openCache() && getFromCache(buildCacheKey(inputParams)) != null;
    }

    private void invokeWithCache(Map<String, Object> inputParams, CRow input, ResultFuture<CRow> resultFuture){
        if (openCache()) {
            CacheObj val = getFromCache(buildCacheKey(inputParams));
            if (val != null) {
                if (ECacheContentType.MissVal == val.getType()) {
                    dealMissKey(input, resultFuture);
                    return;
                }else if(ECacheContentType.SingleLine == val.getType()){
                    try {
                        Row row = fillData(input.row(), val.getContent());
                        resultFuture.complete(Collections.singleton(new CRow(row, input.change())));
                    } catch (Exception e) {
                        dealFillDataError(input, resultFuture, e);
                    }
                } else if (ECacheContentType.MultiLine == val.getType()) {
                    try {
                        List<CRow> rowList = Lists.newArrayList();
                        for (Object one : (List) val.getContent()) {
                            Row row = fillData(input.row(), one);
                            rowList.add(new CRow(row, input.change()));
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

    public abstract void handleAsyncInvoke(Map<String, Object>  inputParams, CRow input, ResultFuture<CRow> resultFuture) throws Exception;

    public abstract String buildCacheKey(Map<String, Object> inputParams);

    private ProcessingTimeService getProcessingTimeService(){
        return ((StreamingRuntimeContext)this.runtimeContext).getProcessingTimeService();
    }

    protected ScheduledFuture<?> registerTimer(CRow input, ResultFuture<CRow> resultFuture){
        long timeoutTimestamp = sideInfo.getSideTableInfo().getAsyncTimeout() + getProcessingTimeService().getCurrentProcessingTime();
        return getProcessingTimeService().registerTimer(
                timeoutTimestamp,
                new ProcessingTimeCallback() {
                    @Override
                    public void onProcessingTime(long timestamp) throws Exception {
                        timeout(input, resultFuture);
                    }
                });
    }

    protected void cancelTimerWhenComplete(ResultFuture<CRow> resultFuture, ScheduledFuture<?> timerFuture){
        if(resultFuture instanceof StreamRecordQueueEntry){
            StreamRecordQueueEntry streamRecordBufferEntry = (StreamRecordQueueEntry) resultFuture;
            streamRecordBufferEntry.onComplete((Object value) -> {
                timerFuture.cancel(true);
            }, cancelExecutor);
        }
    }

    protected void dealFillDataError(CRow input, ResultFuture<CRow> resultFuture, Throwable e) {
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
