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

package com.dtstack.flink.sql.side.redis.table;

import com.dtstack.flink.sql.side.SideTableInfo;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;

public class RedisSideTableInfo extends SideTableInfo {

    private static final long serialVersionUID = -1L;

    private static final String CURR_TYPE = "redis";

    public static final String URL_KEY = "url";

    public static final String PASSWORD_KEY = "password";

    public static final String DATABASE_KEY = "database";

    public static final String TABLE_KEY = "tablename";

    public static final String TIMEOUT = "timeout";

    public static final String MAXTOTAL = "maxTotal";

    public static final String MAXIDLE = "maxIdle";

    public static final String MINIDLE = "minIdle";

    public static final String REDIS_TYPE = "redisType";

    public static final String MASTER_NAME = "masterName";

    private String url;

    private String database;

    private String tableName;

    private String password;

    private int timeout;

    private String maxTotal;

    private String maxIdle;

    private String minIdle;

    private int redisType;

    private String masterName;

    public RedisSideTableInfo(){
        setType(CURR_TYPE);
    }


    @Override
    public boolean check() {
        Preconditions.checkNotNull(url, "redis of URL is required");
        Preconditions.checkNotNull(tableName, "redis of tablename is required");
        Preconditions.checkNotNull(redisType, "redis of type is required");
        return true;
    }

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

    public String getTableName(){
        return tableName;
    }

    public void setTableName(String table){
        this.tableName = table;
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

    public String getMaxTotal() {
        return maxTotal;
    }

    public void setMaxTotal(String maxTotal) {
        this.maxTotal = maxTotal;
    }

    public String getMaxIdle() {
        return maxIdle;
    }

    public void setMaxIdle(String maxIdle) {
        this.maxIdle = maxIdle;
    }

    public String getMinIdle() {
        return minIdle;
    }

    public void setMinIdle(String minIdle) {
        this.minIdle = minIdle;
    }

    public int getRedisType() {
        return redisType;
    }

    public void setRedisType(int redisType) {
        this.redisType = redisType;
    }

    public String getMasterName() {
        return masterName;
    }

    public void setMasterName(String masterName) {
        this.masterName = masterName;
    }
}
