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

package com.dtstack.flink.sql.sink.rdb.resource;

import com.dtstack.flink.sql.resource.ResourceCheck;
import com.dtstack.flink.sql.sink.rdb.table.RdbTableInfo;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * @author: chuixue
 * @create: 2020-12-08 17:24
 * @description:jdbc资源检测
 **/
public class JdbcResourceCheck extends ResourceCheck {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcResourceCheck.class);
    private static final JdbcResourceCheck INSTANCE = new JdbcResourceCheck();

    private JdbcResourceCheck() {
    }

    public static JdbcResourceCheck getInstance() {
        return INSTANCE;
    }

    @Override
    public void checkResourceStatus(AbstractTableInfo abstractTableInfo) {
        RdbTableInfo rdbTableInfo = (RdbTableInfo) abstractTableInfo;
        checkResourceStatus(rdbTableInfo.getName()
                , rdbTableInfo.getDriverName()
                , rdbTableInfo.getUserName()
                , rdbTableInfo.getUrl()
                , rdbTableInfo.getPassword()
                , rdbTableInfo.getSchema()
                , rdbTableInfo.getTableName());
    }

    /**
     * @param name       flinksql中的表名
     * @param driverName 驱动
     * @param username   用户名
     * @param url        地址
     * @param passWord   密码
     * @param schema     schema
     * @param tableName  数据库中的表名
     */
    public void checkResourceStatus(String name
            , String driverName
            , String username
            , String url
            , String passWord
            , String schema
            , String tableName) {
        LOG.info("Start check if table [{}] resource is valid !!!", name);
        Connection connection = null;
        forName(driverName);
        for (int i = 0; i < MAX_RETRY_TIMES; i++) {
            try {
                if (username == null) {
                    connection = DriverManager.getConnection(url);
                } else {
                    connection = DriverManager.getConnection(url, username, passWord);
                }
                if (null != connection) {
                    // 这里不能抛出RuntimeException，用户可能只有某个table的读取权限，而没有读取meta表权限
                    try {
                        // 如果没有异常，说明能有权限读取meta表，如果找不到，则抛出RuntimeException
                        if (!connection.getMetaData().getTables(null, schema, tableName, null).next()) {
                            LOG.error("Table " + tableName + " doesn't exist");
                            throw new SuppressRestartsException(new Throwable("Table " + tableName + " doesn't exist"));
                        }
                    } catch (SQLException e) {
                        LOG.error(e.getMessage());
                    }
                    break;
                }
            } catch (SQLException sqe) {
                if (i == MAX_RETRY_TIMES - 1) {
                    throw new SuppressRestartsException(new Throwable(sqe));
                }
                LOG.error(sqe.getMessage() + " retry " + (i + 1) + " time");
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } finally {
                if (null != connection) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        throw new SuppressRestartsException(new Throwable(e));
                    }
                }
            }
        }
    }

    /**
     * @param clazz
     */
    public synchronized static void forName(String clazz) {
        try {
            Class<?> driverClass = Class.forName(clazz);
            driverClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
