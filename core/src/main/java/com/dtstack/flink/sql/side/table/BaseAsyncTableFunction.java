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

package com.dtstack.flink.sql.side.table;

import com.dtstack.flink.sql.enums.ECacheContentType;
import com.dtstack.flink.sql.enums.ECacheType;
import com.dtstack.flink.sql.metric.MetricConstant;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.ISideReqRow;
import com.dtstack.flink.sql.side.cache.AbstractSideCache;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.cache.LRUSideCache;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.JoinType;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * @author: chuixue
 * @create: 2020-10-12 18:36
 * @description:异步维表公共的类
 **/
abstract public class BaseAsyncTableFunction extends AsyncTableFunction<Row> implements ISideReqRow {
    private static final Logger LOG = LoggerFactory.getLogger(BaseAsyncTableFunction.class);
    private static int TIMEOUT_LOG_FLUSH_NUM = 10;
    private int timeOutNum = 0;
    protected BaseSideInfo sideInfo;
    protected transient Counter parseErrorRecords;
    protected static final int DEFAULT_FETCH_SIZE = 1000;

    public BaseAsyncTableFunction(BaseSideInfo sideInfo) {
        this.sideInfo = sideInfo;
    }

    /**
     * 初始化缓存和metric
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        initCache();
        initMetric(context);
        LOG.info("async dim table config info: {} ", sideInfo.getSideTableInfo().toString());
    }

    /**
     * 初始化缓存
     */
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

    /**
     * 初始化Metric
     *
     * @param context 上下文
     */
    private void initMetric(FunctionContext context) {
        parseErrorRecords = context.getMetricGroup().counter(MetricConstant.DT_NUM_SIDE_PARSE_ERROR_RECORDS);
    }

    /**
     * 通过key得到缓存数据
     *
     * @param key
     * @return
     */
    protected CacheObj getFromCache(String key) {
        return sideInfo.getSideCache().getFromCache(key);
    }

    /**
     * 数据放入缓存
     *
     * @param key
     * @param value
     */
    protected void putCache(String key, CacheObj value) {
        sideInfo.getSideCache().putCache(key, value);
    }

    /**
     * 是否开启缓存
     *
     * @return
     */
    protected boolean openCache() {
        return sideInfo.getSideCache() != null;
    }

    /**
     * 如果缓存获取不到，直接返回空即可，无需判别左/内连接
     *
     * @param future
     */
    public void dealMissKey(CompletableFuture<Collection<Row>> future) {
        try {
            future.complete(Collections.emptyList());
        } catch (Exception e) {
            dealFillDataError(future, e);
        }
    }

    /**
     * 判断是否需要放入缓存
     *
     * @param key
     * @param missKeyObj
     */
    protected void dealCacheData(String key, CacheObj missKeyObj) {
        if (openCache()) {
            putCache(key, missKeyObj);
        }
    }

    // @Override TODO 无法设置超时
    public void timeout(Row input, ResultFuture<Row> resultFuture) throws Exception {

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

    /**
     * 查询前置
     *
     * @param input
     * @param resultFuture
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    protected void preInvoke(Row input, ResultFuture<Row> resultFuture)
            throws InvocationTargetException, IllegalAccessException {
    }

    /**
     * 异步查询数据
     *
     * @param future 发送到下游
     * @param keys   关联数据
     */
    public void eval(CompletableFuture<Collection<Row>> future, Object... keys) throws Exception {
        String cacheKey = buildCacheKey(keys);
        // 缓存判断
        if (isUseCache(cacheKey)) {
            invokeWithCache(cacheKey, future);
            return;
        }
        handleAsyncInvoke(future, keys);
    }

    /**
     * 判断缓存是否存在
     *
     * @param cacheKey 缓存健
     * @return
     */
    protected boolean isUseCache(String cacheKey) {
        return openCache() && getFromCache(cacheKey) != null;
    }

    /**
     * 从缓存中获取数据
     *
     * @param cacheKey 缓存健
     * @param future
     */
    private void invokeWithCache(String cacheKey, CompletableFuture<Collection<Row>> future) {
        if (openCache()) {
            CacheObj val = getFromCache(cacheKey);
            if (val != null) {
                if (ECacheContentType.MissVal == val.getType()) {
                    dealMissKey(future);
                    return;
                } else if (ECacheContentType.SingleLine == val.getType()) {
                    try {
                        Row row = fillData(val.getContent());
                        future.complete(Collections.singleton(row));
                    } catch (Exception e) {
                        dealFillDataError(future, e);
                    }
                } else if (ECacheContentType.MultiLine == val.getType()) {
                    try {
                        List<Row> rowList = Lists.newArrayList();
                        for (Object one : (List) val.getContent()) {
                            Row row = fillData(one);
                            rowList.add(row);
                        }
                        future.complete(rowList);
                    } catch (Exception e) {
                        dealFillDataError(future, e);
                    }
                } else {
                    future.completeExceptionally(new RuntimeException("not support cache obj type " + val.getType()));
                }
                return;
            }
        }
    }

    /**
     * 请求数据库获取数据
     *
     * @param keys   关联字段数据
     * @param future
     * @throws Exception
     */
    public abstract void handleAsyncInvoke(CompletableFuture<Collection<Row>> future, Object... keys) throws Exception;

    /**
     * 构建缓存key值
     *
     * @param keys
     * @return
     */
    public String buildCacheKey(Object... keys) {
        return Arrays.stream(keys)
                .map(Object::toString)
                .collect(Collectors.joining("_"));
    }

    /**
     * 发送异常
     *
     * @param future
     * @param e
     */
    protected void dealFillDataError(CompletableFuture<Collection<Row>> future, Throwable e) {
        parseErrorRecords.inc();
        if (parseErrorRecords.getCount() > sideInfo.getSideTableInfo().getAsyncFailMaxNum(Long.MAX_VALUE)) {
            LOG.info("dealFillDataError", e);
            future.completeExceptionally(e);
        } else {
            dealMissKey(future);
        }
    }

    /**
     * 每次获取的条数
     *
     * @return
     */
    public int getFetchSize() {
        return DEFAULT_FETCH_SIZE;
    }

    /**
     * 资源释放
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public Row fillData(Row input, Object line) {
        return null;
    }
}
