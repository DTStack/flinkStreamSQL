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

package com.dtstack.flink.sql.side.impala;

import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.impala.table.ImpalaSideTableInfo;
import com.dtstack.flink.sql.side.rdb.async.RdbAsyncReqRow;
import com.dtstack.flink.sql.util.KrbUtils;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.types.Row;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedAction;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Date: 2019/11/12
 * Company: www.dtstack.com
 *
 * @author xiuzhu
 */

public class ImpalaAsyncReqRow extends RdbAsyncReqRow {

    private static final Logger LOG = LoggerFactory.getLogger(ImpalaAsyncReqRow.class);

    private final static String IMPALA_DRIVER = "com.cloudera.impala.jdbc41.Driver";

    protected UserGroupInformation ugi = null;

    public ImpalaAsyncReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, AbstractSideTableInfo sideTableInfo) {
        super(new ImpalaAsyncSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ImpalaSideTableInfo impalaSideTableInfo = (ImpalaSideTableInfo) sideInfo.getSideTableInfo();
        if (impalaSideTableInfo.getAuthMech() == 1) {
            String keyTabFilePath = impalaSideTableInfo.getKeyTabFilePath();
            String krb5FilePath = impalaSideTableInfo.getKrb5FilePath();
            String principal = impalaSideTableInfo.getPrincipal();
            ugi = KrbUtils.getUgi(principal, keyTabFilePath, krb5FilePath);
        }
        openJdbc(parameters);
    }

    public void openJdbc(Configuration parameters) throws Exception {
        super.open(parameters);
        ImpalaSideTableInfo impalaSideTableInfo = (ImpalaSideTableInfo) sideInfo.getSideTableInfo();
        JsonObject impalaClientConfig = new JsonObject();
        impalaClientConfig.put("url", getUrl())
                .put("driver_class", IMPALA_DRIVER)
                .put("max_pool_size", impalaSideTableInfo.getAsyncPoolSize())
                .put("provider_class", DT_PROVIDER_CLASS)
                .put("idle_connection_test_period", 300)
                .put("test_connection_on_checkin", DEFAULT_TEST_CONNECTION_ON_CHECKIN)
                .put("max_idle_time", 600)
                .put("preferred_test_query", PREFERRED_TEST_QUERY_SQL)
                .put("idle_connection_test_period", DEFAULT_IDLE_CONNECTION_TEST_PEROID)
                .put("test_connection_on_checkin", DEFAULT_TEST_CONNECTION_ON_CHECKIN);

        System.setProperty("vertx.disableFileCPResolving", "true");

        VertxOptions vo = new VertxOptions();
        vo.setEventLoopPoolSize(DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE);
        vo.setWorkerPoolSize(impalaSideTableInfo.getAsyncPoolSize());
        vo.setFileResolverCachingEnabled(false);
        Vertx vertx = Vertx.vertx(vo);
        setRdbSqlClient(JDBCClient.createNonShared(vertx, impalaClientConfig));
    }

    public String getUrl() {
        ImpalaSideTableInfo impalaSideTableInfo = (ImpalaSideTableInfo) sideInfo.getSideTableInfo();

        String newUrl = "";
        Integer authMech = impalaSideTableInfo.getAuthMech();

        StringBuilder urlBuffer = new StringBuilder(impalaSideTableInfo.getUrl());
        if (authMech == 0) {
            newUrl = urlBuffer.toString();
        } else if (authMech == 1) {
            String krbRealm = impalaSideTableInfo.getKrbRealm();
            String krbHostFQDN = impalaSideTableInfo.getKrbHostFQDN();
            String krbServiceName = impalaSideTableInfo.getKrbServiceName();
            urlBuffer.append(";"
                    .concat("AuthMech=1;")
                    .concat("KrbRealm=").concat(krbRealm).concat(";")
                    .concat("KrbHostFQDN=").concat(krbHostFQDN).concat(";")
                    .concat("KrbServiceName=").concat(krbServiceName).concat(";")
            );
            newUrl = urlBuffer.toString();
        } else if (authMech == 2) {
            String uName = impalaSideTableInfo.getUserName();
            urlBuffer.append(";"
                    .concat("AuthMech=3;")
                    .concat("UID=").concat(uName).concat(";")
                    .concat("PWD=;")
                    .concat("UseSasl=0")
            );
            newUrl = urlBuffer.toString();
        } else if (authMech == 3) {
            String uName = impalaSideTableInfo.getUserName();
            String pwd = impalaSideTableInfo.getPassword();
            urlBuffer.append(";"
                    .concat("AuthMech=3;")
                    .concat("UID=").concat(uName).concat(";")
                    .concat("PWD=").concat(pwd)
            );
            newUrl = urlBuffer.toString();
        } else {
            throw new IllegalArgumentException("The value of authMech is illegal, Please select 0, 1, 2, 3");
        }
        return newUrl;
    }

    @Override
    protected void asyncQueryData(Map<String, Object> inputParams,
                                  CRow input,
                                  ResultFuture<CRow> resultFuture,
                                  SQLClient rdbSqlClient,
                                  AtomicLong failCounter,
                                  AtomicBoolean finishFlag,
                                  CountDownLatch latch) {
        if (ugi == null) {
            doAsyncQueryData(inputParams,
                    input, resultFuture,
                    rdbSqlClient,
                    failCounter,
                    finishFlag,
                    latch);
        } else {
            // Kerberos
            ugi.doAs((PrivilegedAction<Object>) () -> {
                doAsyncQueryData(inputParams,
                        input, resultFuture,
                        rdbSqlClient,
                        failCounter,
                        finishFlag,
                        latch);
                return null;
            });
        }
    }
}
