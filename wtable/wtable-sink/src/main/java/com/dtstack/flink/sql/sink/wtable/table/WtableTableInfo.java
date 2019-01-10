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

 

package com.dtstack.flink.sql.sink.wtable.table;


import com.dtstack.flink.sql.table.TargetTableInfo;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;

public class WtableTableInfo extends TargetTableInfo {

    private static final String CURR_TYPE = "wtable";

    private String nameCenter;

    private String bid;

    private String password;

    private String[] rowkey;

    private Map<String, String> columnNameFamily;

    private String[] columnNames;

    private String[] inputColumnTypes;

    private String[] columnTypes;

    private int tableId = 1;

    private int cachettlms = 0;

    private boolean ignoreRowKeyColumn = false;

    private List<String> primaryKeys;

    public WtableTableInfo(){
        setType(CURR_TYPE);
    }

    public String getNameCenter() {
        return nameCenter;
    }

    public void setNameCenter(String nameCenter) {
        this.nameCenter = nameCenter;
    }

    public String getBid() {
        return bid;
    }

    public void setBid(String bid) {
        this.bid = bid;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getTableId() {
        return tableId;
    }

    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    public int getCachettlms() {
        return cachettlms;
    }

    public void setCachettlms(int cachettlms) {
        this.cachettlms = cachettlms;
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

    public void setIgnoreRowKeyColumn(boolean ignoreRowKeyColumn) {
        this.ignoreRowKeyColumn = ignoreRowKeyColumn;
    }
    public boolean getIgnoreRowKeyColumn() {
        return this.ignoreRowKeyColumn;
    }
    @Override
    public boolean check() {
        Preconditions.checkNotNull(nameCenter, "wtable field of nameCenter is required");
        Preconditions.checkNotNull(bid, "wtable field of bid is required");
        Preconditions.checkNotNull(password, "wtable field of password is required");
        return true;
    }

    @Override
    public String getType() {
        return super.getType().toLowerCase();
    }

    @Override
    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    @Override
    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }
}
