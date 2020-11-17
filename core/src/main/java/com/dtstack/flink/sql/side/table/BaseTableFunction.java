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

import com.dtstack.flink.sql.factory.DTThreadFactory;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.BaseAllReqRow;
import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.ISideReqRow;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @author: chuixue
 * @create: 2020-10-10 19:11
 * @description:全量维表公共的类
 **/
abstract public class BaseTableFunction extends TableFunction<Row> implements ISideReqRow {
    private static final Logger LOG = LoggerFactory.getLogger(BaseAllReqRow.class);

    public static final long LOAD_DATA_ERROR_SLEEP_TIME = 5_000L;

    public static final TimeZone LOCAL_TZ = TimeZone.getDefault();

    protected static final int DEFAULT_FETCH_SIZE = 1000;

    protected static final int CONN_RETRY_NUM = 3;

    protected AtomicReference<Map<String, List<Map<String, Object>>>> cacheRef = new AtomicReference<>();

    protected BaseSideInfo sideInfo;

    private ScheduledExecutorService es;

    public BaseTableFunction(BaseSideInfo sideInfo) {
        this.sideInfo = sideInfo;
    }

    /**
     * 初始化加载数据库中数据
     *
     * @throws SQLException
     */
    protected abstract void initCache() throws SQLException;

    /**
     * 定时加载数据库中数据
     */
    protected abstract void reloadCache();

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        initCache();
        LOG.info("----- all cacheRef init end-----");

        //start reload cache thread
        AbstractSideTableInfo sideTableInfo = sideInfo.getSideTableInfo();
        es = new ScheduledThreadPoolExecutor(1, new DTThreadFactory("cache-all-reload"));
        es.scheduleAtFixedRate(() -> reloadCache()
                , sideTableInfo.getCacheTimeout()
                , sideTableInfo.getCacheTimeout()
                , TimeUnit.MILLISECONDS);
    }

    /**
     * 缓存一行数据
     *
     * @param oneRow   一行数据
     * @param tmpCache 缓存的数据<key ,list<value>>
     */
    protected void buildCache(Map<String, Object> oneRow, Map<String, List<Map<String, Object>>> tmpCache) {
        // 拿到维表字段的物理类型
        String[] lookupKeys = sideInfo.getLookupKeys();
        List<String> physicalFields = Arrays.stream(lookupKeys)
                .map(sideInfo.getSideTableInfo().getPhysicalFields()::get)
                .collect(Collectors.toList());

        String cacheKey = physicalFields.stream()
                .map(oneRow::get)
                .map(Object::toString)
                .collect(Collectors.joining("_"));

        tmpCache.computeIfAbsent(cacheKey, key -> Lists.newArrayList())
                .add(oneRow);
    }

    /**
     * 每条数据都会进入该方法
     *
     * @param keys 维表join key的值
     */
    public void eval(Object... keys) {
        String cacheKey = Arrays.stream(keys)
                .map(Object::toString)
                .collect(Collectors.joining("_"));
        List<Map<String, Object>> cacheList = cacheRef.get().get(cacheKey);
        // 有数据才往下发，(左/内)连接flink会做相应的处理
        if (!CollectionUtils.isEmpty(cacheList)) {
            cacheList.stream().forEach(one -> collect(fillData(one)));
        }
    }

    @Override
    public Row fillData(Object sideInput) {
        Map<String, Object> cacheInfo = (Map<String, Object>) sideInput;
        Collection<String> fields = sideInfo.getSideTableInfo().getPhysicalFields().values();
        String[] fieldsArr = fields.toArray(new String[fields.size()]);
        Row row = new Row(fieldsArr.length);
        for (int i = 0; i < fieldsArr.length; i++) {
            row.setField(i, cacheInfo.get(fieldsArr[i]));
        }
        row.setKind(RowKind.INSERT);
        return row;
    }

    /**
     * 每次获取的条数
     *
     * @return
     */
    public int getFetchSize() {
        return DEFAULT_FETCH_SIZE;
    }

    @Override
    public void close() {
        if (null != es && !es.isShutdown()) {
            es.shutdown();
        }
    }
}
