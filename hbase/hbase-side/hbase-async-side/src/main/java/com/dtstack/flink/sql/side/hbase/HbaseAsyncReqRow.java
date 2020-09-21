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

 

package com.dtstack.flink.sql.side.hbase;

import com.dtstack.flink.sql.side.BaseAsyncReqRow;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.hbase.rowkeydealer.AbstractRowKeyModeDealer;
import com.dtstack.flink.sql.side.hbase.rowkeydealer.PreRowKeyModeDealerDealer;
import com.dtstack.flink.sql.side.hbase.rowkeydealer.RowKeyEqualModeDealer;
import com.dtstack.flink.sql.side.hbase.table.HbaseSideTableInfo;
import com.dtstack.flink.sql.factory.DTThreadFactory;
import com.dtstack.flink.sql.side.hbase.utils.HbaseConfigUtils;
import com.stumbleupon.async.Deferred;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.security.DynamicConfiguration;
import org.apache.flink.runtime.security.KerberosUtils;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.hbase.async.Config;
import org.hbase.async.HBaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.KrbException;

import javax.security.auth.login.AppConfigurationEntry;
import java.io.File;
import java.lang.reflect.Field;
import java.security.PrivilegedExceptionAction;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Date: 2018/8/21
 * Company: www.dtstack.com
 * @author xuchao
 */

public class HbaseAsyncReqRow extends BaseAsyncReqRow {

    private static final long serialVersionUID = 2098635104857937717L;

    private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

    private static final Logger LOG = LoggerFactory.getLogger(HbaseAsyncReqRow.class);

    //match to the rule of netty3
    private static final int DEFAULT_BOSS_THREADS = 1;

    private static final int DEFAULT_IO_THREADS = Runtime.getRuntime().availableProcessors() * 2;

    private static final int DEFAULT_POOL_SIZE = DEFAULT_IO_THREADS + DEFAULT_BOSS_THREADS;

    private transient HBaseClient hBaseClient;

    private transient AbstractRowKeyModeDealer rowKeyMode;

    private String tableName;

    private String[] colNames;

    public HbaseAsyncReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, AbstractSideTableInfo sideTableInfo) {
        super(new HbaseAsyncSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));

        tableName = ((HbaseSideTableInfo)sideTableInfo).getTableName();
        colNames =  StringUtils.split(sideInfo.getSideSelectFields(), ",");
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        AbstractSideTableInfo sideTableInfo = sideInfo.getSideTableInfo();
        HbaseSideTableInfo hbaseSideTableInfo = (HbaseSideTableInfo) sideTableInfo;
        Map<String, Object> hbaseConfig = hbaseSideTableInfo.getHbaseConfig();

        ExecutorService executorService =new ThreadPoolExecutor(DEFAULT_POOL_SIZE, DEFAULT_POOL_SIZE,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), new DTThreadFactory("hbase-async"));

        Config config = new Config();
        config.overrideConfig(HbaseConfigUtils.KEY_HBASE_ZOOKEEPER_QUORUM, hbaseSideTableInfo.getHost());
        config.overrideConfig(HbaseConfigUtils.KEY_HBASE_ZOOKEEPER_ZNODE_QUORUM, hbaseSideTableInfo.getParent());
        HbaseConfigUtils.loadKrb5Conf(hbaseConfig);
        hbaseConfig.entrySet().forEach(entity -> {
            config.overrideConfig(entity.getKey(), (String) entity.getValue());
        });

        if (HbaseConfigUtils.asyncOpenKerberos(hbaseConfig)) {
            String principal = MapUtils.getString(hbaseConfig, HbaseConfigUtils.KEY_PRINCIPAL);
            String keytab = System.getProperty("user.dir") + File.separator + MapUtils.getString(hbaseConfig, HbaseConfigUtils.KEY_KEY_TAB);
            LOG.info("Kerberos login with keytab: {} and principal: {}", keytab, principal);
            String name = "HBaseClient";
            config.overrideConfig("hbase.sasl.clientconfig", name);
            appendJaasConf(name, keytab, principal);
            refreshConfig();
        }

        hBaseClient = new HBaseClient(config, executorService);

        try {
            Deferred deferred = hBaseClient.ensureTableExists(tableName)
                    .addCallbacks(arg -> new CheckResult(true, ""), arg -> new CheckResult(false, arg.toString()));

            CheckResult result = (CheckResult) deferred.join();
            if(!result.isConnect()){
                throw new RuntimeException(result.getExceptionMsg());
            }

        } catch (Exception e) {
            throw new RuntimeException("create hbase connection fail:", e);
        }

        HbaseAsyncSideInfo hbaseAsyncSideInfo = (HbaseAsyncSideInfo) sideInfo;
        if(hbaseSideTableInfo.isPreRowKey()){
            rowKeyMode = new PreRowKeyModeDealerDealer(hbaseAsyncSideInfo.getColRefType(), colNames, hBaseClient,
                    openCache(), sideInfo.getJoinType(), sideInfo.getOutFieldInfoList(),
                    sideInfo.getInFieldIndex(), sideInfo.getSideFieldIndex());
        }else{
            rowKeyMode = new RowKeyEqualModeDealer(hbaseAsyncSideInfo.getColRefType(), colNames, hBaseClient,
                    openCache(), sideInfo.getJoinType(), sideInfo.getOutFieldInfoList(),
                    sideInfo.getInFieldIndex(), sideInfo.getSideFieldIndex());
        }
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

    private void refreshConfig() throws KrbException {
        sun.security.krb5.Config.refresh();
        KerberosName.resetDefaultRealm();
        //reload java.security.auth.login.config
//        javax.security.auth.login.Configuration.setConfiguration(null);
    }

    @Override
    public void handleAsyncInvoke(Map<String, Object> inputParams, CRow input, ResultFuture<CRow> resultFuture) throws Exception {
        rowKeyMode.asyncGetData(tableName, buildCacheKey(inputParams), input, resultFuture, sideInfo.getSideCache());
    }

    @Override
    public String buildCacheKey(Map<String, Object> inputParams) {
        return ((HbaseAsyncSideInfo)sideInfo).getRowKeyBuilder().getRowKey(inputParams);
    }

    @Override
    public Row fillData(Row input, Object sideInput){
        List<Object> sideInputList = (List<Object>) sideInput;
        Row row = new Row(sideInfo.getOutFieldInfoList().size());
        for(Map.Entry<Integer, Integer> entry : sideInfo.getInFieldIndex().entrySet()){
            Object obj = input.getField(entry.getValue());
            boolean isTimeIndicatorTypeInfo = TimeIndicatorTypeInfo.class.isAssignableFrom(sideInfo.getRowTypeInfo().getTypeAt(entry.getValue()).getClass());

            if(obj instanceof Timestamp && isTimeIndicatorTypeInfo){
                //去除上一层OutputRowtimeProcessFunction 调用时区导致的影响
                obj = ((Timestamp) obj).getTime() + (long)LOCAL_TZ.getOffset(((Timestamp) obj).getTime());
            }

            row.setField(entry.getKey(), obj);
        }

        for(Map.Entry<Integer, Integer> entry : sideInfo.getSideFieldIndex().entrySet()){
            if(sideInputList == null){
                row.setField(entry.getKey(), null);
            }else{
                row.setField(entry.getKey(), sideInputList.get(entry.getValue()));
            }
        }

        return row;
    }

    @Override
    public void close() throws Exception {
        super.close();
        hBaseClient.shutdown();
    }

    class CheckResult{

        private boolean connect;

        private String exceptionMsg;

        CheckResult(boolean connect, String msg){
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
