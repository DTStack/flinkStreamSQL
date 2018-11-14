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

package com.dtstack.flink.sql.sink.redis.table;

import com.dtstack.flink.sql.table.TargetTableInfo;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;

public class RedisTableInfo extends TargetTableInfo {

    private static final String CURR_TYPE = "redis";

    public static final String URL_KEY = "url";

    public static final String DATABASE_KEY = "database";

    public static final String PASSWORD_KEY = "password";

    public static final String TABLENAME_KEY = "tablename";

    public static final String TIMEOUT = "timeout";

    public RedisTableInfo(){
        setType(CURR_TYPE);
    }

    private String url;

    private String database;

    private String tableName;

    private String password;

    private int timeout = 1000;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    @Override
    public boolean check() {
        Preconditions.checkNotNull(url, "redis field of URL is required");
        Preconditions.checkNotNull(database, "redis field of database is required");
        Preconditions.checkNotNull(password, "redis field of password is required");
        return true;
    }

    @Override
    public String getType() {
        return super.getType().toLowerCase();
    }

    public String getTablename() {
        return tableName;
    }

    public void setTablename(String tablename) {
        this.tableName = tablename;
    }
}
