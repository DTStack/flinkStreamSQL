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

import com.dtstack.flink.sql.threadFactory.DTThreadFactory;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Reason:
 * Date: 2018/9/18
 * Company: www.dtstack.com
 * @author xuchao
 */

public abstract class AllReqRow extends RichFlatMapFunction<Row, Row> implements ISideReqRow {

    protected SideInfo sideInfo;

    private ScheduledExecutorService es;

    public AllReqRow(SideInfo sideInfo){
        this.sideInfo = sideInfo;

    }

    protected abstract void initCache() throws SQLException;

    protected abstract void reloadCache();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        initCache();
        System.out.println("----- all cacheRef init end-----");

        //start reload cache thread
        SideTableInfo sideTableInfo = sideInfo.getSideTableInfo();
        es = Executors.newSingleThreadScheduledExecutor(new DTThreadFactory("cache-all-reload"));
        es.scheduleAtFixedRate(() -> reloadCache(), sideTableInfo.getCacheTimeout(), sideTableInfo.getCacheTimeout(), TimeUnit.MILLISECONDS);
    }

}
