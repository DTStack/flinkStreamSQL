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


package com.dtstack.flink.sql.side.sqlserver;

import com.dtstack.flink.sql.side.*;
import com.dtstack.flink.sql.side.rdb.async.RdbAsyncReqRow;
import com.dtstack.flink.sql.side.rdb.table.RdbSideTableInfo;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class SqlserverAsyncReqRow extends RdbAsyncReqRow {

    private static final Logger LOG = LoggerFactory.getLogger(SqlserverAsyncReqRow.class);

    private final static String SQLSERVER_DRIVER = "net.sourceforge.jtds.jdbc.Driver";

    public SqlserverAsyncReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        JsonObject sqlserverClientConfig = new JsonObject();
        RdbSideTableInfo rdbSideTableInfo = (RdbSideTableInfo) sideInfo.getSideTableInfo();
        sqlserverClientConfig.put("url", rdbSideTableInfo.getUrl())
                .put("driver_class", SQLSERVER_DRIVER)
                .put("max_pool_size", DEFAULT_MAX_DB_CONN_POOL_SIZE)
                .put("user", rdbSideTableInfo.getUserName())
                .put("password", rdbSideTableInfo.getPassword());

        VertxOptions vo = new VertxOptions();
        vo.setEventLoopPoolSize(DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE);
        vo.setWorkerPoolSize(DEFAULT_VERTX_WORKER_POOL_SIZE);
        Vertx vertx = Vertx.vertx(vo);
        setRdbSQLClient(JDBCClient.createNonShared(vertx, sqlserverClientConfig));
    }
}
