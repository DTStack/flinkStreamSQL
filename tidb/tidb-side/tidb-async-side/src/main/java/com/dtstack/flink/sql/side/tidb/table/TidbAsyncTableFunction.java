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

package com.dtstack.flink.sql.side.tidb.table;

import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.rdb.async.RdbAsyncTableFunction;
import com.dtstack.flink.sql.side.rdb.table.RdbSideTableInfo;
import com.dtstack.flink.sql.side.tidb.TidbAsyncSideInfo;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import org.apache.flink.table.functions.FunctionContext;

/**
 * @author: chuixue
 * @create: 2020-10-27 15:25
 * @description:
 **/
public class TidbAsyncTableFunction extends RdbAsyncTableFunction {
    private final static String TIDB_DRIVER = "com.mysql.jdbc.Driver";

    public TidbAsyncTableFunction(AbstractSideTableInfo sideTableInfo, String[] lookupKeys) {
        super(new TidbAsyncSideInfo(sideTableInfo, lookupKeys));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
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
    }
}
