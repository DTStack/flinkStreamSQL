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

import com.dtstack.flink.sql.enums.ECacheContentType;
import com.dtstack.flink.sql.side.AsyncReqRow;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.SideTableInfo;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.hbase.rowkeydealer.AbsRowKeyModeDealer;
import com.dtstack.flink.sql.side.hbase.rowkeydealer.PreRowKeyModeDealerDealer;
import com.dtstack.flink.sql.side.hbase.rowkeydealer.RowKeyEqualModeDealer;
import com.dtstack.flink.sql.side.hbase.table.HbaseSideTableInfo;
import com.dtstack.flink.sql.factory.DTThreadFactory;
import com.dtstack.flink.sql.side.hbase.utils.HbaseConfigUtils;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.hbase.async.Config;
import org.hbase.async.HBaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Date: 2018/8/21
 * Company: www.dtstack.com
 * @author xuchao
 */

public class HbaseAsyncReqRow extends AsyncReqRow {

    private static final long serialVersionUID = 2098635104857937717L;

    private static final Logger LOG = LoggerFactory.getLogger(HbaseAsyncReqRow.class);

    //match to the rule of netty3
    private static final int DEFAULT_BOSS_THREADS = 1;

    private static final int DEFAULT_IO_THREADS = Runtime.getRuntime().availableProcessors() * 2;

    private static final int DEFAULT_POOL_SIZE = DEFAULT_IO_THREADS + DEFAULT_BOSS_THREADS;

    private transient HBaseClient hBaseClient;

    private transient AbsRowKeyModeDealer rowKeyMode;

    private String tableName;

    private String[] colNames;

    public HbaseAsyncReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(new HbaseAsyncSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));

        tableName = ((HbaseSideTableInfo)sideTableInfo).getTableName();
        colNames = ((HbaseSideTableInfo)sideTableInfo).getColumnRealNames();
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        SideTableInfo sideTableInfo = sideInfo.getSideTableInfo();
        HbaseSideTableInfo hbaseSideTableInfo = (HbaseSideTableInfo) sideTableInfo;
        Map<String, Object> hbaseConfig = hbaseSideTableInfo.getHbaseConfig();

        ExecutorService executorService =new ThreadPoolExecutor(DEFAULT_POOL_SIZE, DEFAULT_POOL_SIZE,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), new DTThreadFactory("hbase-aysnc"));

        Config config = new Config();
        config.overrideConfig(HbaseConfigUtils.KEY_HBASE_ZOOKEEPER_QUORUM, hbaseSideTableInfo.getHost());
        config.overrideConfig(HbaseConfigUtils.KEY_HBASE_ZOOKEEPER_ZNODE_QUORUM, hbaseSideTableInfo.getParent());
        HbaseConfigUtils.loadKrb5Conf(hbaseConfig);
        hbaseConfig.entrySet().forEach(entity -> {
            config.overrideConfig(entity.getKey(), (String) entity.getValue());
        });

        if (HbaseConfigUtils.asyncOpenKerberos(hbaseConfig)) {
            String jaasStr = HbaseConfigUtils.buildJaasStr(hbaseConfig);
            String jaasFilePath = HbaseConfigUtils.creatJassFile(jaasStr);
            config.overrideConfig(HbaseConfigUtils.KEY_JAVA_SECURITY_AUTH_LOGIN_CONF, jaasFilePath);
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

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {
        Map<String, Object> refData = Maps.newHashMap();
        for (int i = 0; i < sideInfo.getEqualValIndex().size(); i++) {
            Integer conValIndex = sideInfo.getEqualValIndex().get(i);
            Object equalObj = input.getField(conValIndex);
            if(equalObj == null){
                dealMissKey(input, resultFuture);
                return;
            }

            refData.put(sideInfo.getEqualFieldList().get(i), equalObj);
        }

        String rowKeyStr = ((HbaseAsyncSideInfo)sideInfo).getRowKeyBuilder().getRowKey(refData);

        //get from cache
        if(openCache()){
            CacheObj val = getFromCache(rowKeyStr);
            if(val != null){
                if(ECacheContentType.MissVal == val.getType()){
                    dealMissKey(input, resultFuture);
                    return;
                }else if(ECacheContentType.SingleLine == val.getType()){
                    Row row = fillData(input, val);
                    resultFuture.complete(Collections.singleton(row));
                }else if(ECacheContentType.MultiLine == val.getType()){
                    for(Object one : (List)val.getContent()){
                        Row row = fillData(input, one);
                        resultFuture.complete(Collections.singleton(row));
                    }
                }
                return;
            }
        }

        rowKeyMode.asyncGetData(tableName, rowKeyStr, input, resultFuture, sideInfo.getSideCache());
    }

    @Override
    public Row fillData(Row input, Object sideInput){

        List<Object> sideInputList = (List<Object>) sideInput;
        Row row = new Row(sideInfo.getOutFieldInfoList().size());
        for(Map.Entry<Integer, Integer> entry : sideInfo.getInFieldIndex().entrySet()){
            Object obj = input.getField(entry.getValue());
            obj = convertTimeIndictorTypeInfo(entry.getValue(), obj);
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
        if (null!=hBaseClient) {
            hBaseClient.shutdown();
        }
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
