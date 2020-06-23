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

package com.dtstack.flink.sql.side.tidb;

import com.dtstack.flink.sql.factory.DTThreadFactory;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.rdb.async.RdbAsyncReqRow;
import com.dtstack.flink.sql.side.rdb.table.RdbSideTableInfo;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author tiezhu
 * Date 2020/6/1
 * company www.dtstack.com
 */
public class TidbAsyncReqRow extends RdbAsyncReqRow {

    private final static String TIDB_DRIVER = "com.mysql.jdbc.Driver";

    public TidbAsyncReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, AbstractSideTableInfo sideTableInfo) {
        super(new TidbAsyncSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        JsonObject tidbClientConfig = new JsonObject();
        RdbSideTableInfo sideTableInfo = (RdbSideTableInfo) sideInfo.getSideTableInfo();
        tidbClientConfig.put("url", sideTableInfo.getUrl())
                .put("driver_class", TIDB_DRIVER)
                .put("max_pool_size", sideTableInfo.getAsyncPoolSize())
                .put("user", sideTableInfo.getUserName())
                .put("password", sideTableInfo.getPassword())
                .put("provider_class", DT_PROVIDER_CLASS)
                .put("preferred_test_query", PREFERRED_TEST_QUERY_SQL)
                .put("test_connection_on_checkin", DEFAULT_TEST_CONNECTION_ON_CHECKIN)
                .put("idle_connection_test_period", DEFAULT_IDLE_CONNECTION_TEST_PEROID);

        System.setProperty("vertx.disableFileCPResolving", "true");

        VertxOptions vo = new VertxOptions();
        vo.setEventLoopPoolSize(DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE);
        vo.setWorkerPoolSize(sideTableInfo.getAsyncPoolSize());
        vo.setFileResolverCachingEnabled(false);
        Vertx vertx = Vertx.vertx(vo);
        setRdbSqlClient(JDBCClient.createNonShared(vertx, tidbClientConfig));
        setExecutor(new ThreadPoolExecutor(50, 50, 0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(10000), new DTThreadFactory("TidbAsyncExec"), new ThreadPoolExecutor.CallerRunsPolicy()));
    }
}
