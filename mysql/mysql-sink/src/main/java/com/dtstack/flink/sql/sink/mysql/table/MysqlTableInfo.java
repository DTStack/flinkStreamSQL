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

 

package com.dtstack.flink.sql.sink.mysql.table;

import com.dtstack.flink.sql.table.TargetTableInfo;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;

/**
 * Reason:
 * Date: 2018/6/22
 * Company: www.dtstack.com
 * @author xuchao
 */

public class MysqlTableInfo extends TargetTableInfo {

    private static final String CURR_TYPE = "mysql";

    public static final String URL_KEY = "url";

    public static final String TABLE_NAME_KEY = "tableName";

    public static final String USER_NAME_KEY = "userName";

    public static final String PASSWORD_KEY = "password";

    public static final String BATCH_SIZE_KEY = "batchSize";

    public static final String BUFFER_SIZE_KEY = "bufferSize";

    public static final String FLUSH_INTERVALMS_KEY = "flushIntervalMs";

    public MysqlTableInfo(){
        setType(CURR_TYPE);
    }

    private String url;

    private String tableName;

    private String userName;

    private String password;

    private Integer batchSize;

    private String bufferSize;

    private int flushIntervalMs;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public String getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(String bufferSize) {
        this.bufferSize = bufferSize;
    }

    public int getFlushIntervalMs() {
        return flushIntervalMs;
    }

    public void setFlushIntervalMs(int flushIntervalMs) {
        this.flushIntervalMs = flushIntervalMs;
    }

    @Override
    public boolean check() {
        Preconditions.checkNotNull(url, "mysql field of URL is required");
        Preconditions.checkNotNull(tableName, "mysql field of tableName is required");
        Preconditions.checkNotNull(userName, "mysql field of userName is required");
        Preconditions.checkNotNull(password, "mysql field of password is required");
        return true;
    }

    @Override
    public String getType() {
        // return super.getType().toLowerCase() + TARGET_SUFFIX;
        return super.getType().toLowerCase();
    }
}
