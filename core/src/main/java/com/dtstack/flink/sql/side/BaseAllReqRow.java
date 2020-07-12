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

import com.dtstack.flink.sql.util.RowDataComplete;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.flink.sql.factory.DTThreadFactory;
import org.apache.calcite.sql.JoinType;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Reason:
 * Date: 2018/9/18
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public abstract class BaseAllReqRow extends RichFlatMapFunction<Tuple2<Boolean, Row>, Tuple2<Boolean, BaseRow>> implements ISideReqRow {

    private static final Logger LOG = LoggerFactory.getLogger(BaseAllReqRow.class);

    public static final long LOAD_DATA_ERROR_SLEEP_TIME = 5_000L;

    protected BaseSideInfo sideInfo;

    private ScheduledExecutorService es;

    public BaseAllReqRow(BaseSideInfo sideInfo) {
        this.sideInfo = sideInfo;

    }

    protected abstract void initCache() throws SQLException;

    protected abstract void reloadCache();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        initCache();
        LOG.info("----- all cacheRef init end-----");

        //start reload cache thread
        AbstractSideTableInfo sideTableInfo = sideInfo.getSideTableInfo();
        es = new ScheduledThreadPoolExecutor(1, new DTThreadFactory("cache-all-reload"));
        es.scheduleAtFixedRate(() -> reloadCache(), sideTableInfo.getCacheTimeout(), sideTableInfo.getCacheTimeout(), TimeUnit.MILLISECONDS);
    }

    protected Object convertTimeIndictorTypeInfo(Integer index, Object obj) {
        boolean isTimeIndicatorTypeInfo = TimeIndicatorTypeInfo.class.isAssignableFrom(sideInfo.getRowTypeInfo().getTypeAt(index).getClass());

        //Type information for indicating event or processing time. However, it behaves like a regular SQL timestamp but is serialized as Long.
        if (obj instanceof LocalDateTime && isTimeIndicatorTypeInfo) {
            obj = Timestamp.valueOf(((LocalDateTime) obj));
        }
        return obj;
    }

    protected void sendOutputRow(Tuple2<Boolean, Row> value, Object sideInput, Collector<Tuple2<Boolean, BaseRow>> out) {
        if (sideInput == null && sideInfo.getJoinType() != JoinType.LEFT) {
            return;
        }
        Row row = fillData(value.f1, sideInput);
        RowDataComplete.collectTupleRow(out, Tuple2.of(value.f0, row));
    }

    @Override
    public void close() throws Exception {
        if (null != es && !es.isShutdown()) {
            es.shutdown();
        }
    }
}
