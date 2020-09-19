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

 

package com.dtstack.flink.sql.sink.hbase.table;


import com.dtstack.flink.sql.enums.EUpdateMode;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.table.AbstractTableParser;
import com.dtstack.flink.sql.util.DtStringUtil;
import com.dtstack.flink.sql.util.MathUtil;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

import static com.dtstack.flink.sql.table.AbstractTableInfo.PARALLELISM_KEY;

/**
 * Date: 2018/09/14
 * Company: www.dtstack.com
 * @author sishu.yss
 */
public class HbaseSinkParser extends AbstractTableParser {


    public static final String HBASE_ZOOKEEPER_QUORUM = "zookeeperQuorum";

    public static final String ZOOKEEPER_PARENT = "zookeeperParent";

    public static final String HBASE_ROWKEY = "rowkey";

    public static final String TABLE_NAME_KEY = "tableName";

    public static final String UPDATE_KEY = "updateMode";

    public static final String KERBEROS_AUTH_ENABLE_KEY = "kerberosAuthEnable";
    public static final String REGIONSERVER_KEYTAB_FILE_KEY = "regionserverKeytabFile";
    public static final String REGIONSERVER_PRINCIPAL_KEY = "regionserverPrincipal";
    public static final String SECURITY_KRB5_CONF_KEY = "securityKrb5Conf";
    public static final String ZOOKEEPER_SASL_CLINT_KEY = "zookeeperSaslClient";

    public static final String CLIENT_PRINCIPAL_KEY = "clientPrincipal";
    public static final String CLIENT_KEYTABFILE_KEY = "clientKeytabFile";

    public static final String BATCH_SIZE = "batchSize";
    public static final String BATCH_WAIT_INTERVAL = "batchWaitInterval";

    @Override
    protected boolean fieldNameNeedsUpperCase() {
        return false;
    }

    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        HbaseTableInfo hbaseTableInfo = new HbaseTableInfo();
        hbaseTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, hbaseTableInfo);
        hbaseTableInfo.setTableName((String) props.get(TABLE_NAME_KEY.toLowerCase()));
        hbaseTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(PARALLELISM_KEY.toLowerCase())));
        hbaseTableInfo.setHost((String) props.get(HBASE_ZOOKEEPER_QUORUM.toLowerCase()));
        hbaseTableInfo.setParent((String)props.get(ZOOKEEPER_PARENT.toLowerCase()));
        String rk = (String) props.get(HBASE_ROWKEY.toLowerCase());
        hbaseTableInfo.setRowkey(rk);
        String updateMode = (String) props.getOrDefault(UPDATE_KEY, EUpdateMode.APPEND.name());
        hbaseTableInfo.setUpdateMode(updateMode);

        hbaseTableInfo.setKerberosAuthEnable(MathUtil.getBoolean(props.get(KERBEROS_AUTH_ENABLE_KEY.toLowerCase()), false));
        hbaseTableInfo.setRegionserverKeytabFile((String) props.get(REGIONSERVER_KEYTAB_FILE_KEY.toLowerCase()));
        hbaseTableInfo.setRegionserverPrincipal((String) props.get(REGIONSERVER_PRINCIPAL_KEY.toLowerCase()));
        hbaseTableInfo.setSecurityKrb5Conf((String) props.get(SECURITY_KRB5_CONF_KEY.toLowerCase()));
        hbaseTableInfo.setZookeeperSaslClient((String) props.get(ZOOKEEPER_SASL_CLINT_KEY.toLowerCase()));

        hbaseTableInfo.setClientPrincipal((String) props.get(CLIENT_PRINCIPAL_KEY.toLowerCase()));
        hbaseTableInfo.setClientKeytabFile((String) props.get(CLIENT_KEYTABFILE_KEY.toLowerCase()));

        hbaseTableInfo.setBatchSize(MathUtil.getString(props.getOrDefault(BATCH_SIZE.toLowerCase(), "100")));
        hbaseTableInfo.setBatchWaitInterval(MathUtil.getString(props.getOrDefault(BATCH_WAIT_INTERVAL.toLowerCase(), "1000")));

        return hbaseTableInfo;
    }

    public void parseFieldsInfo(String fieldsInfo, HbaseTableInfo tableInfo){
        List<String> fieldRows = DtStringUtil.splitIgnoreQuota(fieldsInfo, ',');
        for(String fieldRow : fieldRows){
            fieldRow = fieldRow.trim();

            String[] filedInfoArr = fieldRow.split("\\s+");
            if(filedInfoArr.length < 2 ){
                throw new RuntimeException(String.format("table [%s] field [%s] format error.", tableInfo.getName(), fieldRow));
            }

            boolean isMatcherKey = dealKeyPattern(fieldRow, tableInfo);
            if(isMatcherKey){
                continue;
            }

            //Compatible situation may arise in space in the fieldName
            String[] filedNameArr = new String[filedInfoArr.length - 1];
            System.arraycopy(filedInfoArr, 0, filedNameArr, 0, filedInfoArr.length - 1);
            String fieldName = String.join(" ", filedNameArr);
            String fieldType = filedInfoArr[filedInfoArr.length - 1 ].trim();
            Class fieldClass = dbTypeConvertToJavaType(fieldType);
            String[] columnFamily = StringUtils.split(fieldName.trim(), ":");
            tableInfo.addPhysicalMappings(filedInfoArr[0],filedInfoArr[0]);
            tableInfo.addField(columnFamily[1]);
            tableInfo.addFieldClass(fieldClass);
            tableInfo.addFieldType(fieldType);
            tableInfo.addFieldExtraInfo(null);
        }
        tableInfo.setColumnNameFamily(parseColumnFamily(tableInfo.getPhysicalFields()));
        tableInfo.finish();
    }

    private Map<String, String> parseColumnFamily(Map<String, String> physicalFieldMap){
        Map<String, String> columnFamiles = Maps.newLinkedHashMap();
        physicalFieldMap.values().forEach(x -> {
            String[] columnFamily = StringUtils.split(x.trim(), ":");
            columnFamiles.put(x, columnFamily[1]);
        });

        return columnFamiles;
    }
}
