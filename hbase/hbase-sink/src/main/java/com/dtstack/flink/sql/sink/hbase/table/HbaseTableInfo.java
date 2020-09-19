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


import com.dtstack.flink.sql.table.AbstractTargetTableInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Date: 2018/09/14
 * Company: www.dtstack.com
 * @author sishu.yss
 */
public class HbaseTableInfo extends AbstractTargetTableInfo {

    private static final String CURR_TYPE = "hbase";

    private String host;

    private String port;

    private String parent;

    private String rowkey;

    private Map<String, String> columnNameFamily;

    private String[] columnNames;

    private String[] inputColumnTypes;

    private String[] columnTypes;

    private String tableName;

    private String updateMode;

    private boolean kerberosAuthEnable;

    private String regionserverKeytabFile;

    private String regionserverPrincipal;

    private String securityKrb5Conf;

    private String zookeeperSaslClient;

    private String clientPrincipal;

    private String clientKeytabFile;

    private String batchSize;

    private String batchWaitInterval;

    private Map<String,Object> hbaseConfig = Maps.newHashMap();

    public HbaseTableInfo(){
        setType(CURR_TYPE);
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getParent() {
        return parent;
    }

    public void setParent(String parent) {
        this.parent = parent;
    }

    public String getRowkey() {
        return rowkey;
    }

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
    }

    public Map<String, String> getColumnNameFamily() {
        return columnNameFamily;
    }

    public void setColumnNameFamily(Map<String, String> columnNameFamily) {
        this.columnNameFamily = columnNameFamily;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
    }

    public String[] getInputColumnTypes() {
        return inputColumnTypes;
    }

    public void setInputColumnTypes(String[] inputColumnTypes) {
        this.inputColumnTypes = inputColumnTypes;
    }

    public String[] getColumnTypes() {
        return columnTypes;
    }

    public void setColumnTypes(String[] columnTypes) {
        this.columnTypes = columnTypes;
    }

    public String getUpdateMode() {
        return updateMode;
    }

    public void setUpdateMode(String updateMode) {
        this.updateMode = updateMode;
    }

    @Override
    public boolean check() {
        Preconditions.checkNotNull(host, "hbase field of zookeeperQuorum is required");
        return true;
    }

    @Override
    public String getType() {
        return super.getType().toLowerCase();
    }


    public Map<String, Object> getHbaseConfig() {
        return hbaseConfig;
    }

    public void setHbaseConfig(Map<String, Object> hbaseConfig) {
        this.hbaseConfig = hbaseConfig;
    }

    public boolean isKerberosAuthEnable() {
        return kerberosAuthEnable;
    }

    public void setKerberosAuthEnable(boolean kerberosAuthEnable) {
        this.kerberosAuthEnable = kerberosAuthEnable;
    }

    public String getRegionserverKeytabFile() {
        return regionserverKeytabFile;
    }

    public void setRegionserverKeytabFile(String regionserverKeytabFile) {
        this.regionserverKeytabFile = regionserverKeytabFile;
    }

    public String getRegionserverPrincipal() {
        return regionserverPrincipal;
    }

    public void setRegionserverPrincipal(String regionserverPrincipal) {
        this.regionserverPrincipal = regionserverPrincipal;
    }

    public String getSecurityKrb5Conf() {
        return securityKrb5Conf;
    }

    public void setSecurityKrb5Conf(String securityKrb5Conf) {
        this.securityKrb5Conf = securityKrb5Conf;
    }

    public String getZookeeperSaslClient() {
        return zookeeperSaslClient;
    }

    public void setZookeeperSaslClient(String zookeeperSaslClient) {
        this.zookeeperSaslClient = zookeeperSaslClient;
    }

    public String getClientPrincipal() {
        return clientPrincipal;
    }

    public void setClientPrincipal(String clientPrincipal) {
        this.clientPrincipal = clientPrincipal;
    }

    public String getClientKeytabFile() {
        return clientKeytabFile;
    }

    public void setClientKeytabFile(String clientKeytabFile) {
        this.clientKeytabFile = clientKeytabFile;
    }

    public String getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(String batchSize) {
        this.batchSize = batchSize;
    }

    public String getBatchWaitInterval() {
        return batchWaitInterval;
    }

    public void setBatchWaitInterval(String batchWaitInterval) {
        this.batchWaitInterval = batchWaitInterval;
    }
}
