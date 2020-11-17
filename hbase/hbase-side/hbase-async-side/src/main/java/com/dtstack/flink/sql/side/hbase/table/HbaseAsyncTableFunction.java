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

package com.dtstack.flink.sql.side.hbase.table;

import com.dtstack.flink.sql.factory.DTThreadFactory;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.hbase.HbaseAsyncSideInfo;
import com.dtstack.flink.sql.side.hbase.rowkeydealer.AbstractRowKeyModeDealer;
import com.dtstack.flink.sql.side.hbase.rowkeydealer.PreRowKeyModeDealerDealer;
import com.dtstack.flink.sql.side.hbase.rowkeydealer.RowKeyEqualModeDealer;
import com.dtstack.flink.sql.side.hbase.utils.HbaseConfigUtils;
import com.dtstack.flink.sql.side.table.BaseAsyncTableFunction;
import com.dtstack.flink.sql.util.DataTypeUtils;
import com.dtstack.flink.sql.util.DtFileUtils;
import com.stumbleupon.async.Deferred;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.runtime.security.DynamicConfiguration;
import org.apache.flink.runtime.security.KerberosUtils;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.hbase.async.Config;
import org.hbase.async.HBaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.KrbException;

import javax.security.auth.login.AppConfigurationEntry;
import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author: chuixue
 * @create: 2020-10-29 16:08
 * @description:
 **/
public class HbaseAsyncTableFunction extends BaseAsyncTableFunction {

    private static final long serialVersionUID = 2098635104857937717L;

    private static final Logger LOG = LoggerFactory.getLogger(HbaseAsyncTableFunction.class);

    //match to the rule of netty3
    private static final int DEFAULT_BOSS_THREADS = 1;

    private static final int DEFAULT_IO_THREADS = Runtime.getRuntime().availableProcessors() * 2;

    private static final int DEFAULT_POOL_SIZE = DEFAULT_IO_THREADS + DEFAULT_BOSS_THREADS;

    private transient HBaseClient hBaseClient;

    private transient AbstractRowKeyModeDealer rowKeyMode;

    private String tableName;

    private String[] colNames;

    public HbaseAsyncTableFunction(AbstractSideTableInfo sideTableInfo, String[] lookupKeys) {
        super(new HbaseAsyncSideInfo(sideTableInfo, lookupKeys));

        tableName = ((HbaseSideTableInfo) sideTableInfo).getTableName();
        colNames = DataTypeUtils.getPhysicalFieldNames(sideInfo.getSideTableInfo());
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        AbstractSideTableInfo sideTableInfo = sideInfo.getSideTableInfo();
        HbaseSideTableInfo hbaseSideTableInfo = (HbaseSideTableInfo) sideTableInfo;
        Map<String, Object> hbaseConfig = hbaseSideTableInfo.getHbaseConfig();

        ExecutorService executorService = new ThreadPoolExecutor(DEFAULT_POOL_SIZE, DEFAULT_POOL_SIZE,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), new DTThreadFactory("hbase-async"));

        Config config = new Config();
        config.overrideConfig(HbaseConfigUtils.KEY_HBASE_ZOOKEEPER_QUORUM, hbaseSideTableInfo.getHost());
        config.overrideConfig(HbaseConfigUtils.KEY_HBASE_ZOOKEEPER_ZNODE_QUORUM, hbaseSideTableInfo.getParent());
        hbaseConfig.entrySet().forEach(entity -> {
            config.overrideConfig(entity.getKey(), (String) entity.getValue());
        });

        if (HbaseConfigUtils.isEnableKerberos(hbaseConfig)) {
            HbaseConfigUtils.loadKrb5Conf(hbaseConfig);
            String principal = MapUtils.getString(hbaseConfig, HbaseConfigUtils.KEY_PRINCIPAL);
            HbaseConfigUtils.checkOpt(principal, HbaseConfigUtils.KEY_PRINCIPAL);
            String regionserverPrincipal = MapUtils.getString(hbaseConfig, HbaseConfigUtils.KEY_HBASE_KERBEROS_REGIONSERVER_PRINCIPAL);
            HbaseConfigUtils.checkOpt(regionserverPrincipal, HbaseConfigUtils.KEY_HBASE_KERBEROS_REGIONSERVER_PRINCIPAL);
            String keytab = MapUtils.getString(hbaseConfig, HbaseConfigUtils.KEY_KEY_TAB);
            HbaseConfigUtils.checkOpt(keytab, HbaseConfigUtils.KEY_KEY_TAB);
            String keytabPath = System.getProperty("user.dir") + File.separator + keytab;
            DtFileUtils.checkExists(keytabPath);

            LOG.info("Kerberos login with keytab: {} and principal: {}", keytab, principal);
            String name = "HBaseClient";
            config.overrideConfig("hbase.sasl.clientconfig", name);
            appendJaasConf(name, keytab, principal);
            refreshConfig();
        }

        hBaseClient = new HBaseClient(config, executorService);

        try {
            Deferred deferred = hBaseClient.ensureTableExists(tableName)
                    .addCallbacks(arg -> new HbaseAsyncTableFunction.CheckResult(true, ""), arg -> new HbaseAsyncTableFunction.CheckResult(false, arg.toString()));

            HbaseAsyncTableFunction.CheckResult result = (HbaseAsyncTableFunction.CheckResult) deferred.join();
            if (!result.isConnect()) {
                throw new RuntimeException(result.getExceptionMsg());
            }

        } catch (Exception e) {
            throw new RuntimeException("create hbase connection fail:", e);
        }

        HbaseAsyncSideInfo hbaseAsyncSideInfo = (HbaseAsyncSideInfo) sideInfo;
        if (hbaseSideTableInfo.isPreRowKey()) {
            rowKeyMode = new PreRowKeyModeDealerDealer(hbaseAsyncSideInfo.getColRefType()
                    , colNames
                    , hBaseClient
                    , openCache()
                    , sideInfo
                    , this);
        } else {
            rowKeyMode = new RowKeyEqualModeDealer(hbaseAsyncSideInfo.getColRefType()
                    , colNames
                    , hBaseClient
                    , openCache()
                    , sideInfo
                    , this);
        }
    }

    private void refreshConfig() throws KrbException {
        sun.security.krb5.Config.refresh();
        KerberosName.resetDefaultRealm();
        //reload java.security.auth.login.config
        // javax.security.auth.login.Configuration.setConfiguration(null);
    }

    private void appendJaasConf(String name, String keytab, String principal) {
        javax.security.auth.login.Configuration priorConfig = javax.security.auth.login.Configuration.getConfiguration();
        // construct a dynamic JAAS configuration
        DynamicConfiguration currentConfig = new DynamicConfiguration(priorConfig);
        // wire up the configured JAAS login contexts to use the krb5 entries
        AppConfigurationEntry krb5Entry = KerberosUtils.keytabEntry(keytab, principal);
        currentConfig.addAppConfigurationEntry(name, krb5Entry);
        javax.security.auth.login.Configuration.setConfiguration(currentConfig);
    }

    @Override
    public void handleAsyncInvoke(CompletableFuture<Collection<Row>> future, Object... keys) throws Exception {
        rowKeyMode.asyncGetData(tableName, buildCacheKey(keys), future, sideInfo.getSideCache());
    }

    @Override
    public String buildCacheKey(Object... keys) {
        HbaseAsyncSideInfo hbaseAsyncSideInfo = (HbaseAsyncSideInfo) this.sideInfo;
        String[] lookupKeys = hbaseAsyncSideInfo.getLookupKeys();
        Map<String, Object> refData = IntStream
                .range(0, lookupKeys.length)
                .boxed()
                .collect(Collectors.toMap(i -> lookupKeys[i], i -> keys[i]));
        return ((HbaseAsyncSideInfo) sideInfo).getRowKeyBuilder().getRowKey(refData);
    }

    @Override
    public Row fillData(Object sideInput) {
        List<Object> sideInputList = (List<Object>) sideInput;
        Row row = new Row(colNames.length);
        for (int i = 0; i < colNames.length; i++) {
            Object object = sideInputList.get(i);
            row.setField(i, object);
        }
        row.setKind(RowKind.INSERT);
        return row;
    }

    @Override
    public void close() throws Exception {
        super.close();
        hBaseClient.shutdown();
    }

    class CheckResult {

        private boolean connect;

        private String exceptionMsg;

        CheckResult(boolean connect, String msg) {
            this.connect = connect;
            this.exceptionMsg = msg;
        }

        public boolean isConnect() {
            return connect;
        }

        public void setConnect(boolean connect) {
            this.connect = connect;
        }

        public String getExceptionMsg() {
            return exceptionMsg;
        }

        public void setExceptionMsg(String exceptionMsg) {
            this.exceptionMsg = exceptionMsg;
        }
    }
}
