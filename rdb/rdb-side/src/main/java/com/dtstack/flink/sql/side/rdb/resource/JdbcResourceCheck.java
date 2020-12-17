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

package com.dtstack.flink.sql.side.rdb.resource;

import com.dtstack.flink.sql.resource.ResourceCheck;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.rdb.table.RdbSideTableInfo;
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
    private static final JdbcResourceCheck Instance = new JdbcResourceCheck();

    private JdbcResourceCheck() {
    }

    public static JdbcResourceCheck getInstance() {
        return Instance;
    }

    @Override
    public void checkResourceStatus(AbstractSideTableInfo abstractSideTableInfo) {
        RdbSideTableInfo rdbSideTableInfo = (RdbSideTableInfo) abstractSideTableInfo;
        Connection connection = null;
        forName(rdbSideTableInfo.getDriverName());
        for (int i = 0; i < MAX_RETRY_TIMES; i++) {
            try {
                if (rdbSideTableInfo.getUserName() == null) {
                    connection = DriverManager.getConnection(rdbSideTableInfo.getUrl());
                } else {
                    connection = DriverManager.getConnection(rdbSideTableInfo.getUrl(), rdbSideTableInfo.getUserName(), rdbSideTableInfo.getPassword());
                }
                if (null != connection) {
                    // 这里不能抛出RuntimeException，用户可能只有某个table的读取权限，而没有读取meta表权限
                    try {
                        // 如果没有异常，说明能有权限读取meta表，如果找不到，则抛出RuntimeException
                        if (!connection.getMetaData().getTables(null, rdbSideTableInfo.getSchema(), rdbSideTableInfo.getTableName(), null).next()) {
                            LOG.error("Table " + rdbSideTableInfo.getTableName() + " doesn't exist");
                            throw new SuppressRestartsException(new Throwable("Table " + rdbSideTableInfo.getTableName() + " doesn't exist"));
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
