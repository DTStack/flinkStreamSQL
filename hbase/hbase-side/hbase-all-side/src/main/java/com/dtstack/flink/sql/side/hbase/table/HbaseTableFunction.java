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

import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.hbase.HbaseAllSideInfo;
import com.dtstack.flink.sql.side.hbase.utils.HbaseConfigUtils;
import com.dtstack.flink.sql.side.hbase.utils.HbaseUtils;
import com.dtstack.flink.sql.side.table.BaseTableFunction;
import com.dtstack.flink.sql.util.DataTypeUtils;
import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author: chuixue
 * @create: 2020-10-29 15:26
 * @description:
 **/
public class HbaseTableFunction extends BaseTableFunction {

    private static final Logger LOG = LoggerFactory.getLogger(HbaseTableFunction.class);

    private String tableName;

    private Map<String, String> aliasNameInversion;

    private AtomicReference<Map<String, Map<String, Object>>> cacheRef = new AtomicReference<>();
    private Connection conn = null;
    private Table table = null;
    private ResultScanner resultScanner = null;
    private Configuration conf = null;

    public HbaseTableFunction(AbstractSideTableInfo sideTableInfo, String[] lookupKeys) {
        super(new HbaseAllSideInfo(sideTableInfo, lookupKeys));
        tableName = ((HbaseSideTableInfo) sideTableInfo).getTableName();

        HbaseSideTableInfo hbaseSideTableInfo = (HbaseSideTableInfo) sideTableInfo;
        Map<String, String> aliasNameRef = hbaseSideTableInfo.getAliasNameRef();
        aliasNameInversion = new HashMap<>();
        for (Map.Entry<String, String> entry : aliasNameRef.entrySet()) {
            aliasNameInversion.put(entry.getValue(), entry.getKey());
        }
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

    /**
     * 每条数据都会进入该方法
     *
     * @param keys 维表join key的值
     */
    @Override
    public void eval(Object... keys) {
        HbaseAllSideInfo hbaseAllSideInfo = ((HbaseAllSideInfo) sideInfo);
        String[] lookupKeys = hbaseAllSideInfo.getLookupKeys();
        Map<String, Object> refData = IntStream
                .range(0, lookupKeys.length)
                .boxed()
                .collect(Collectors.toMap(i -> lookupKeys[i], i -> keys[i]));
        String rowKeyStr = hbaseAllSideInfo.getRowKeyBuilder().getRowKey(refData);
        Map<String, Object> cacheList = cacheRef.get().get(rowKeyStr);
        // 有数据才往下发，(左/内)连接flink会做相应的处理
        if (!MapUtils.isEmpty(cacheList)) {
            collect(fillData(cacheList));
        }
    }

    @Override
    public Row fillData(Object sideInput) {
        Map<String, Object> cacheInfo = (Map<String, Object>) sideInput;
        Collection<String> fields = new ArrayList<>(Arrays.asList(DataTypeUtils.getFieldNames(sideInfo.getSideTableInfo())));
        String[] fieldsArr = fields.toArray(new String[fields.size()]);
        Row row = new Row(fieldsArr.length);
        for (int i = 0; i < fieldsArr.length; i++) {
            row.setField(i, cacheInfo.get(fieldsArr[i]));
        }
        row.setKind(RowKind.INSERT);
        return row;
    }

    private void loadData(Map<String, Map<String, Object>> tmpCache) throws SQLException {
        AbstractSideTableInfo sideTableInfo = sideInfo.getSideTableInfo();
        Map<String, String> colRefType = ((HbaseAllSideInfo) sideInfo).getColRefType();
        HbaseSideTableInfo hbaseSideTableInfo = (HbaseSideTableInfo) sideTableInfo;
        boolean openKerberos = hbaseSideTableInfo.isKerberosAuthEnable();
        int loadDataCount = 0;
        try {
            if (openKerberos) {
                conf = HbaseConfigUtils.getHadoopConfiguration(hbaseSideTableInfo.getHbaseConfig());
                conf.set(HbaseConfigUtils.KEY_HBASE_ZOOKEEPER_QUORUM, hbaseSideTableInfo.getHost());
                conf.set(HbaseConfigUtils.KEY_HBASE_ZOOKEEPER_ZNODE_QUORUM, hbaseSideTableInfo.getParent());
                String principal = HbaseConfigUtils.getPrincipal(hbaseSideTableInfo.getHbaseConfig());
                String keytab = HbaseConfigUtils.getKeytab(hbaseSideTableInfo.getHbaseConfig());

                UserGroupInformation userGroupInformation = HbaseConfigUtils.loginAndReturnUGI(conf, principal, keytab);
                Configuration finalConf = conf;
                conn = userGroupInformation.doAs(new PrivilegedAction<Connection>() {
                    @Override
                    public Connection run() {
                        try {
                            return ConnectionFactory.createConnection(finalConf);
                        } catch (IOException e) {
                            LOG.error("Get connection fail with config:{}", finalConf);
                            throw new RuntimeException(e);
                        }
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
                for (String primaryKey : hbaseSideTableInfo.getPrimaryKeys()) {
                    kv.put(primaryKey, HbaseUtils.convertByte(r.getRow(), "string"));
                }
                loadDataCount++;
                tmpCache.put(new String(r.getRow()), kv);
            }
        } catch (IOException e) {
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
