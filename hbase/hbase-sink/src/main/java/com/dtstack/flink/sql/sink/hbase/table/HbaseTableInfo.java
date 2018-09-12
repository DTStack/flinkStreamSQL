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


import com.dtstack.flink.sql.table.TargetTableInfo;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;

import java.util.Map;

public class HbaseTableInfo extends TargetTableInfo {

    private static final String CURR_TYPE = "hbase";

    private String host;

    private String port;

    private String parent;

    private String[] rowkey;

    private Map<String, String> columnNameFamily;

    private String[] columnNames;

    private String[] inputColumnTypes;

    private String[] columnTypes;

    private String tableName;

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

    @Override
    public boolean check() {
        Preconditions.checkNotNull(host, "hbase field of zookeeperQuorum is required");
        return true;
    }

    @Override
    public String getType() {
        return super.getType().toLowerCase();
    }

}
