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

import com.dtstack.flink.sql.side.SideTableInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * hbase-dimensional form of property
 * Date: 2018/8/21
 * Company: www.dtstack.com
 * @author xuchao
 */
public class HbaseSideTableInfo extends SideTableInfo {

    private static final String CURR_TYPE = "hbase";

    private String host;

    private String port;

    private String parent;

    private String[] rowkey;

    /**是否根据rowkey前缀查询*/
    private boolean preRowKey = false;

    private Map<String, String> columnNameFamily;

    private String tableName;

    private String[] columnRealNames;

    private List<String> columnRealNameList = Lists.newArrayList();

    private Map<String, String> aliasNameRef = Maps.newHashMap();

    private Map<String,String> hbaseParam = Maps.newHashMap();

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

    public String getHbaseParam(String key){
        return hbaseParam.get(key);
    }

    public Set<String> getHbaseParamKeys(){
        return hbaseParam.keySet();
    }

    public void addHbaseParam(String key,String value){
        hbaseParam.put(key,value);
    }

    public Map<String, String> getHbaseParam(){
        return hbaseParam;
    }

    public void setHbaseParam(Map<String, String> hbaseParam){
        this.hbaseParam = hbaseParam;
    }


    @Override
    public void finish(){
        super.finish();
        this.columnRealNames = columnRealNameList.toArray(new String[columnRealNameList.size()]);
    }
}
