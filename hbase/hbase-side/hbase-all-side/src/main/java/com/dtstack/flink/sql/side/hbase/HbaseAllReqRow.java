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

import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.BaseAllReqRow;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.hbase.table.HbaseSideTableInfo;
import com.dtstack.flink.sql.side.hbase.utils.HbaseConfigUtils;
import com.dtstack.flink.sql.side.hbase.utils.HbaseUtils;
import com.dtstack.flink.sql.util.RowDataComplete;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.JoinType;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.KrbException;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class HbaseAllReqRow extends BaseAllReqRow {

    private static final Logger LOG = LoggerFactory.getLogger(HbaseAllReqRow.class);

    private String tableName;

    private Map<String, String> aliasNameInversion;

    private AtomicReference<Map<String, Map<String, Object>>> cacheRef = new AtomicReference<>();
    private Connection conn = null;
    private Table table = null;
    private ResultScanner resultScanner = null;

    public HbaseAllReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, AbstractSideTableInfo sideTableInfo) {
        super(new HbaseAllSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
        tableName = ((HbaseSideTableInfo) sideTableInfo).getTableName();

        HbaseSideTableInfo hbaseSideTableInfo = (HbaseSideTableInfo) sideTableInfo;
        Map<String, String> aliasNameRef = hbaseSideTableInfo.getAliasNameRef();
        aliasNameInversion = new HashMap<>();
        for (Map.Entry<String, String> entry : aliasNameRef.entrySet()) {
            aliasNameInversion.put(entry.getValue(), entry.getKey());
        }
    }

    @Override
    public Row fillData(Row input, Object sideInput) {
        Map<String, Object> sideInputList = (Map<String, Object>) sideInput;
        Row row = new Row(sideInfo.getOutFieldInfoList().size());
        for (Map.Entry<Integer, Integer> entry : sideInfo.getInFieldIndex().entrySet()) {
            Object obj = input.getField(entry.getValue());
            boolean isTimeIndicatorTypeInfo = TimeIndicatorTypeInfo.class.isAssignableFrom(sideInfo.getRowTypeInfo().getTypeAt(entry.getValue()).getClass());

            //Type information for indicating event or processing time. However, it behaves like a regular SQL timestamp but is serialized as Long.
            if (obj instanceof LocalDateTime && isTimeIndicatorTypeInfo) {
                //去除上一层OutputRowtimeProcessFunction 调用时区导致的影响
                obj = ((Timestamp) obj).getTime() + (long) LOCAL_TZ.getOffset(((Timestamp) obj).getTime());
            }

            row.setField(entry.getKey(), obj);
        }

        for (Map.Entry<Integer, Integer> entry : sideInfo.getSideFieldIndex().entrySet()) {
            if (sideInputList == null) {
                row.setField(entry.getKey(), null);
            } else {
                String key = sideInfo.getSideFieldNameIndex().get(entry.getKey());
                key = aliasNameInversion.get(key);
                row.setField(entry.getKey(), sideInputList.get(key));
            }
        }
        return row;
    }

    @Override
    protected void initCache() throws SQLException {
        Map<String, Map<String, Object>> newCache = Maps.newConcurrentMap();
        cacheRef.set(newCache);
        loadData(newCache);
    }

    @Override
    protected void reloadCache() {
        Map<String, Map<String, Object>> newCache = Maps.newConcurrentMap();
        try {
            loadData(newCache);
        } catch (SQLException e) {
            LOG.error("", e);
        }

        cacheRef.set(newCache);
        LOG.info("----- HBase all cacheRef reload end:{}", Calendar.getInstance());
    }

    @Override
    public void flatMap(Row input, Collector<Row> out) throws Exception {
        Map<String, Object> refData = Maps.newHashMap();
        for (int i = 0; i < sideInfo.getEqualValIndex().size(); i++) {
            Integer conValIndex = sideInfo.getEqualValIndex().get(i);
            Object equalObj = input.getField(conValIndex);
            if (equalObj == null) {
                if (sideInfo.getJoinType() == JoinType.LEFT) {
                    Row data = fillData(input, null);
                    out.collect(data);
                }
                return;
            }
            refData.put(sideInfo.getEqualFieldList().get(i), equalObj);
        }

        String rowKeyStr = ((HbaseAllSideInfo) sideInfo).getRowKeyBuilder().getRowKey(refData);

        Map<String, Object> cacheList = null;

        AbstractSideTableInfo sideTableInfo = sideInfo.getSideTableInfo();
        HbaseSideTableInfo hbaseSideTableInfo = (HbaseSideTableInfo) sideTableInfo;
        if (hbaseSideTableInfo.isPreRowKey()) {
            for (Map.Entry<String, Map<String, Object>> entry : cacheRef.get().entrySet()) {
                if (entry.getKey().startsWith(rowKeyStr)) {
                    cacheList = cacheRef.get().get(entry.getKey());
                    Row row = fillData(input, cacheList);
                    out.collect(row);
                }
            }
        } else {
            cacheList = cacheRef.get().get(rowKeyStr);
            Row row = fillData(input, cacheList);
            out.collect(row);
        }

    }

    private void loadData(Map<String, Map<String, Object>> tmpCache) throws SQLException {
        AbstractSideTableInfo sideTableInfo = sideInfo.getSideTableInfo();
        Map<String, String> colRefType = ((HbaseAllSideInfo) sideInfo).getColRefType();
        HbaseSideTableInfo hbaseSideTableInfo = (HbaseSideTableInfo) sideTableInfo;
        boolean openKerberos = hbaseSideTableInfo.isKerberosAuthEnable();
        Configuration conf;
        int loadDataCount = 0;
        try {
            if (openKerberos) {
                conf = HbaseConfigUtils.getHadoopConfiguration(hbaseSideTableInfo.getHbaseConfig());
                conf.set(HbaseConfigUtils.KEY_HBASE_ZOOKEEPER_QUORUM, hbaseSideTableInfo.getHost());
                conf.set(HbaseConfigUtils.KEY_HBASE_ZOOKEEPER_ZNODE_QUORUM, hbaseSideTableInfo.getParent());

                String principal = HbaseConfigUtils.getPrincipal(hbaseSideTableInfo.getHbaseConfig());
                String keytab = HbaseConfigUtils.getKeytab(hbaseSideTableInfo.getHbaseConfig());

                HbaseConfigUtils.fillSyncKerberosConfig(conf, hbaseSideTableInfo.getHbaseConfig());
                keytab = System.getProperty("user.dir") + File.separator + keytab;

                LOG.info("kerberos principal:{}，keytab:{}", principal, keytab);

                conf.set(HbaseConfigUtils.KEY_HBASE_CLIENT_KEYTAB_FILE, keytab);
                conf.set(HbaseConfigUtils.KEY_HBASE_CLIENT_KERBEROS_PRINCIPAL, principal);

                UserGroupInformation userGroupInformation = HbaseConfigUtils.loginAndReturnUGI2(conf, principal, keytab);
                Configuration finalConf = conf;
                conn = userGroupInformation.doAs((PrivilegedAction<Connection>) () -> {
                    try {
                        ScheduledChore authChore = AuthUtil.getAuthChore(finalConf);
                        if (authChore != null) {
                            ChoreService choreService = new ChoreService("hbaseKerberosSink");
                            choreService.scheduleChore(authChore);
                        }

                        return ConnectionFactory.createConnection(finalConf);

                    } catch (IOException e) {
                        LOG.error("Get connection fail with config:{}", finalConf);
                        throw new RuntimeException(e);
                    }
                });

            } else {
                conf = HbaseConfigUtils.getConfig(hbaseSideTableInfo.getHbaseConfig());
                conf.set(HbaseConfigUtils.KEY_HBASE_ZOOKEEPER_QUORUM, hbaseSideTableInfo.getHost());
                conf.set(HbaseConfigUtils.KEY_HBASE_ZOOKEEPER_ZNODE_QUORUM, hbaseSideTableInfo.getParent());
                conn = ConnectionFactory.createConnection(conf);
            }

            table = conn.getTable(TableName.valueOf(tableName));
            resultScanner = table.getScanner(new Scan());
            for (Result r : resultScanner) {
                Map<String, Object> kv = new HashedMap();
                for (Cell cell : r.listCells()) {
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    StringBuilder key = new StringBuilder();
                    key.append(family).append(":").append(qualifier);
                    Object value = HbaseUtils.convertByte(CellUtil.cloneValue(cell), colRefType.get(key.toString()));
                    kv.put(aliasNameInversion.get(key.toString()), value);
                }
                loadDataCount++;
                tmpCache.put(new String(r.getRow()), kv);
            }
        } catch (IOException | KrbException e) {
            throw new RuntimeException(e);
        } finally {
            LOG.info("load Data count: {}", loadDataCount);
            try {
                if (null != conn) {
                    conn.close();
                }

                if (null != table) {
                    table.close();
                }

                if (null != resultScanner) {
                    resultScanner.close();
                }
            } catch (IOException e) {
                LOG.error("", e);
            }
        }
    }

}