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

 

package com.dtstack.flink.sql.side.wtable.table;

import com.dtstack.flink.sql.side.SideTableInfo;
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
public class WtableSideTableInfo extends SideTableInfo {

    private static final String CURR_TYPE = "wtable";

    private String nameCenter;

    private String bid;

    private String password;

    private String[] rowkey;

    /**是否根据rowkey前缀查询*/
    private boolean preRowKey = false;

    private Map<String, String> columnNameFamily;

    private int tableId = 1;

    public WtableSideTableInfo(){
        setType(CURR_TYPE);
    }

    @Override
    public boolean check() {
        return false;
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

    public boolean isPreRowKey() {
        return preRowKey;
    }

    public void setPreRowKey(boolean preRowKey) {
        this.preRowKey = preRowKey;
    }

    @Override
    public void finish(){
        super.finish();
    }
}
