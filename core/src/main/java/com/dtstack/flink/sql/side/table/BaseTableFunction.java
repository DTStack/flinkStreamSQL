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
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.TimeZone;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author: chuixue
 * @create: 2020-10-10 19:11
 * @description:全量维表公共的类
 **/
abstract public class BaseTableFunction extends TableFunction<Row> implements ISideReqRow {
    private static final Logger LOG = LoggerFactory.getLogger(BaseAllReqRow.class);

    public static final long LOAD_DATA_ERROR_SLEEP_TIME = 5_000L;

    public static final TimeZone LOCAL_TZ = TimeZone.getDefault();

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

    protected Object convertTimeIndictorTypeInfo(Integer index, Object obj) {
        boolean isTimeIndicatorTypeInfo = TimeIndicatorTypeInfo.class.isAssignableFrom(sideInfo.getRowTypeInfo().getTypeAt(index).getClass());

        //Type information for indicating event or processing time. However, it behaves like a regular SQL timestamp but is serialized as Long.
        if (obj instanceof LocalDateTime && isTimeIndicatorTypeInfo) {
            //去除上一层OutputRowtimeProcessFunction 调用时区导致的影响
            obj = ((Timestamp) obj).getTime() + (long) LOCAL_TZ.getOffset(((Timestamp) obj).getTime());
        }
        return obj;
    }

    @Override
    public void close() {
        if (null != es && !es.isShutdown()) {
            es.shutdown();
        }
    }
}
