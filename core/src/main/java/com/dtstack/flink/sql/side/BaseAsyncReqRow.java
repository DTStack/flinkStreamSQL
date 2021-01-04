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
import com.dtstack.flink.sql.util.RowDataComplete;
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
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ScheduledFuture;

/**
 * All interfaces inherit naming rules: type + "AsyncReqRow" such as == "MysqlAsyncReqRow
 * only support Left join / inner join(join),not support right join
 * Date: 2018/7/9
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public abstract class BaseAsyncReqRow extends RichAsyncFunction<BaseRow, BaseRow> implements ISideReqRow {
    private static final Logger LOG = LoggerFactory.getLogger(BaseAsyncReqRow.class);
    private static final long serialVersionUID = 2098635244857937717L;
    private RuntimeContext runtimeContext;
    private static int TIMEOUT_LOG_FLUSH_NUM = 10;
    private int timeOutNum = 0;
    protected BaseSideInfo sideInfo;
    protected transient Counter parseErrorRecords;
    private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

    public BaseAsyncReqRow(BaseSideInfo sideInfo) {
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

    private void initCache() {
        AbstractSideTableInfo sideTableInfo = sideInfo.getSideTableInfo();
        if (sideTableInfo.getCacheType() == null || ECacheType.NONE.name().equalsIgnoreCase(sideTableInfo.getCacheType())) {
            return;
        }

        AbstractSideCache sideCache;
        if (ECacheType.LRU.name().equalsIgnoreCase(sideTableInfo.getCacheType())) {
            sideCache = new LRUSideCache(sideTableInfo);
            sideInfo.setSideCache(sideCache);
        } else {
            throw new RuntimeException("not support side cache with type:" + sideTableInfo.getCacheType());
        }

        sideCache.initCache();
    }

    private void initMetric() {
        parseErrorRecords = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_SIDE_PARSE_ERROR_RECORDS);
    }


    protected Object convertTimeIndictorTypeInfo(Integer index, Object obj) {
        boolean isTimeIndicatorTypeInfo = TimeIndicatorTypeInfo.class.isAssignableFrom(sideInfo.getRowTypeInfo().getTypeAt(index).getClass());

        //Type information for indicating event or processing time. However, it behaves like a regular SQL timestamp but is serialized as Long.
        if (obj instanceof LocalDateTime && isTimeIndicatorTypeInfo) {
            //去除上一层OutputRowtimeProcessFunction 调用时区导致的影响
            obj = ((Timestamp) obj).getTime() + (long)LOCAL_TZ.getOffset(((Timestamp) obj).getTime());
        }
        return obj;
    }

    protected CacheObj getFromCache(String key) {
        return sideInfo.getSideCache().getFromCache(key);
    }

    protected void putCache(String key, CacheObj value) {
        sideInfo.getSideCache().putCache(key, value);
    }

    protected boolean openCache() {
        return sideInfo.getSideCache() != null;
    }

    protected void dealMissKey(BaseRow input, ResultFuture<BaseRow> resultFuture) {
        if (sideInfo.getJoinType() == JoinType.LEFT) {
            //Reserved left table data
            try {
                BaseRow row = fillData(input, null);
                RowDataComplete.completeBaseRow(resultFuture, row);
            } catch (Exception e) {
                dealFillDataError(input, resultFuture, e);
            }
        } else {
            resultFuture.complete(Collections.EMPTY_LIST);
        }
    }

    protected void dealCacheData(String key, CacheObj missKeyObj) {
        if (openCache()) {
            putCache(key, missKeyObj);
        }
    }

    @Override
    public void timeout(BaseRow input, ResultFuture<BaseRow> resultFuture) throws Exception {

        if (timeOutNum % TIMEOUT_LOG_FLUSH_NUM == 0) {
            LOG.info("Async function call has timed out. input:{}, timeOutNum:{}", input.toString(), timeOutNum);
        }
        timeOutNum++;
        if (sideInfo.getJoinType() == JoinType.LEFT) {
            resultFuture.complete(Collections.EMPTY_LIST);
            return;
        }
        if (timeOutNum > sideInfo.getSideTableInfo().getAsyncFailMaxNum(Long.MAX_VALUE)) {
            resultFuture.completeExceptionally(new Exception("Async function call timedoutNum beyond limit."));
            return;
        }
        resultFuture.complete(Collections.EMPTY_LIST);
    }

    protected void preInvoke(BaseRow input, ResultFuture<BaseRow> resultFuture)
            throws InvocationTargetException, IllegalAccessException {
        registerTimerAndAddToHandler(input, resultFuture);
    }

    @Override
    public void asyncInvoke(BaseRow row, ResultFuture<BaseRow> resultFuture) throws Exception {
        preInvoke(row, resultFuture);
        Map<String, Object> inputParams = parseInputParam(row);
        if (MapUtils.isEmpty(inputParams)) {
            dealMissKey(row, resultFuture);
            return;
        }
        if (isUseCache(inputParams)) {
            invokeWithCache(inputParams, row, resultFuture);
            return;
        }
        handleAsyncInvoke(inputParams, row, resultFuture);
    }

    private Map<String, Object> parseInputParam(BaseRow input) {
        GenericRow genericRow = (GenericRow) input;
        Map<String, Object> inputParams = Maps.newLinkedHashMap();
        for (int i = 0; i < sideInfo.getEqualValIndex().size(); i++) {
            Integer conValIndex = sideInfo.getEqualValIndex().get(i);
            Object equalObj = genericRow.getField(conValIndex);
            if (equalObj == null) {
                return inputParams;
            }
            String columnName = sideInfo.getEqualFieldList().get(i);
            inputParams.put(columnName, equalObj);
        }
        return inputParams;
    }

    protected boolean isUseCache(Map<String, Object> inputParams) {
        return openCache() && getFromCache(buildCacheKey(inputParams)) != null;
    }

    private void invokeWithCache(Map<String, Object> inputParams, BaseRow input, ResultFuture<BaseRow> resultFuture) {
        if (openCache()) {
            CacheObj val = getFromCache(buildCacheKey(inputParams));
            if (val != null) {
                if (ECacheContentType.MissVal == val.getType()) {
                    dealMissKey(input, resultFuture);
                    return;
                } else if (ECacheContentType.SingleLine == val.getType()) {
                    try {
                        BaseRow row = fillData(input, val.getContent());
                        RowDataComplete.completeBaseRow(resultFuture, row);
                    } catch (Exception e) {
                        dealFillDataError(input, resultFuture, e);
                    }
                } else if (ECacheContentType.MultiLine == val.getType()) {
                    try {
                        List<BaseRow> rowList = Lists.newArrayList();
                        for (Object one : (List) val.getContent()) {
                            BaseRow row = fillData(input, one);
                            rowList.add(row);
                        }
                        RowDataComplete.completeBaseRow(resultFuture,rowList);
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

    public abstract void handleAsyncInvoke(Map<String, Object> inputParams, BaseRow input, ResultFuture<BaseRow> resultFuture) throws Exception;

    public abstract String buildCacheKey(Map<String, Object> inputParams);

    private ProcessingTimeService getProcessingTimeService() {
        return ((StreamingRuntimeContext) this.runtimeContext).getProcessingTimeService();
    }

    protected ScheduledFuture<?> registerTimer(BaseRow input, ResultFuture<BaseRow> resultFuture) {
        long timeoutTimestamp = sideInfo.getSideTableInfo().getAsyncTimeout() + getProcessingTimeService().getCurrentProcessingTime();
        return getProcessingTimeService().registerTimer(
                timeoutTimestamp,
                timestamp -> timeout(input, resultFuture));
    }

    protected void registerTimerAndAddToHandler(BaseRow input, ResultFuture<BaseRow> resultFuture)
            throws InvocationTargetException, IllegalAccessException {
        ScheduledFuture<?> timeFuture = registerTimer(input, resultFuture);
        // resultFuture 是ResultHandler 的实例
        Method setTimeoutTimer = ReflectionUtils.getDeclaredMethod(resultFuture, "setTimeoutTimer", ScheduledFuture.class);
        setTimeoutTimer.setAccessible(true);
        setTimeoutTimer.invoke(resultFuture, timeFuture);
    }


    protected void dealFillDataError(BaseRow input, ResultFuture<BaseRow> resultFuture, Throwable e) {
        parseErrorRecords.inc();
        if (parseErrorRecords.getCount() > sideInfo.getSideTableInfo().getAsyncFailMaxNum(Long.MAX_VALUE)) {
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
