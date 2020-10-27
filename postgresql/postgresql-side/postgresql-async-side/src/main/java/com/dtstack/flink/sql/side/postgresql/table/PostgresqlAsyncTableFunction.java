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

package com.dtstack.flink.sql.side.postgresql.table;

import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.postgresql.PostgresqlAsyncSideInfo;
import com.dtstack.flink.sql.side.rdb.async.RdbAsyncTableFunction;
import com.dtstack.flink.sql.side.rdb.table.RdbSideTableInfo;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import org.apache.flink.table.functions.FunctionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: chuixue
 * @create: 2020-10-27 15:23
 * @description:
 **/
public class PostgresqlAsyncTableFunction extends RdbAsyncTableFunction {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresqlAsyncTableFunction.class);

    private final static String POSTGRESQL_DRIVER = "org.postgresql.Driver";

    public PostgresqlAsyncTableFunction(AbstractSideTableInfo sideTableInfo, String[] lookupKeyso) {
        super(new PostgresqlAsyncSideInfo(sideTableInfo, lookupKeyso));
    }


    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        JsonObject pgClientConfig = new JsonObject();
        RdbSideTableInfo rdbSideTableInfo = (RdbSideTableInfo) sideInfo.getSideTableInfo();
        pgClientConfig.put("url", rdbSideTableInfo.getUrl())
                .put("driver_class", POSTGRESQL_DRIVER)
                .put("max_pool_size", rdbSideTableInfo.getAsyncPoolSize())
                .put("user", rdbSideTableInfo.getUserName())
                .put("password", rdbSideTableInfo.getPassword())
                .put("preferred_test_query", PREFERRED_TEST_QUERY_SQL)
                .put("idle_connection_test_period", DEFAULT_IDLE_CONNECTION_TEST_PEROID)
                .put("test_connection_on_checkin", DEFAULT_TEST_CONNECTION_ON_CHECKIN);

        VertxOptions vo = new VertxOptions();
        vo.setEventLoopPoolSize(DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE);
        vo.setWorkerPoolSize(rdbSideTableInfo.getAsyncPoolSize());
        Vertx vertx = Vertx.vertx(vo);
        setRdbSqlClient(JDBCClient.createNonShared(vertx, pgClientConfig));
    }
}
