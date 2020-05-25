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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * hbase-dimensional form of property
 * Date: 2018/8/21
 * Company: www.dtstack.com
 * @author xuchao
 */
public class HbaseSideTableInfo extends AbstractSideTableInfo {

    private static final String CURR_TYPE = "hbase";

    private String host;

    private String port;

    private String parent;

    private String[] rowkey;

    /**是否根据rowkey前缀查询*/
    private boolean preRowKey = false;

    private Map<String, String> columnNameFamily;

    private String tableName;

    private boolean kerberosAuthEnable;

    private String regionserverKeytabFile;

    private String regionserverPrincipal;

    private String jaasPrincipal;

    private String securityKrb5Conf;

    private String zookeeperSaslClient;

    private String[] columnRealNames;

    private List<String> columnRealNameList = Lists.newArrayList();

    private Map<String, String> aliasNameRef = Maps.newHashMap();

    public HbaseSideTableInfo(){
        setType(CURR_TYPE);
    }


    @Override
    public boolean check() {
        return false;
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

    public String[] getRowkey() {
        return rowkey;
    }

    public void setRowkey(String[] rowkey) {
        this.rowkey = rowkey;
    }

    public Map<String, String> getColumnNameFamily() {
        return columnNameFamily;
    }

    public void setColumnNameFamily(Map<String, String> columnNameFamily) {
        this.columnNameFamily = columnNameFamily;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void putAliasNameRef(String aliasName, String hbaseField){
        aliasNameRef.put(aliasName, hbaseField);
    }

    public Map<String, String> getAliasNameRef() {
        return aliasNameRef;
    }

    public String getHbaseField(String fieldAlias){
        return aliasNameRef.get(fieldAlias);
    }

    public String[] getColumnRealNames() {
        return columnRealNames;
    }

    public void setColumnRealNames(String[] columnRealNames) {
        this.columnRealNames = columnRealNames;
    }

    public void addColumnRealName(String realName){
        this.columnRealNameList.add(realName);
    }

    public boolean isPreRowKey() {
        return preRowKey;
    }

    public void setPreRowKey(boolean preRowKey) {
        this.preRowKey = preRowKey;
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

    public String getJaasPrincipal() {
        return jaasPrincipal;
    }

    public void setJaasPrincipal(String jaasPrincipal) {
        this.jaasPrincipal = jaasPrincipal;
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

    @Override
    public void finish(){
        super.finish();
        this.columnRealNames = columnRealNameList.toArray(new String[columnRealNameList.size()]);
    }
}
