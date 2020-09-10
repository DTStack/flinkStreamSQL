/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flink.sql.dirty.mysql;

import com.dtstack.flink.sql.dirtyManager.consumer.AbstractDirtyDataConsumer;
import com.dtstack.flink.sql.dirtyManager.entity.DirtyDataEntity;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author tiezhu
 * Company dtstack
 * Date 2020/8/27 星期四
 */
public class MysqlDirtyDataConsumer extends AbstractDirtyDataConsumer {
    //TODO 添加batchSize 和 定时任务
    private static final long serialVersionUID = -2959753658786001679L;

    private static final String DRIVER_NAME = "com.mysql.jdbc.Driver";

    private final Object LOCK_STR = new Object();

    private final String[] tableField = {"id", "dirtyData", "processTime", "cause", "field"};

    private final String SQL = "INSERT INTO ? (?, ?, ?, ?) VALUES (?, ?, ?, ?) ";

    private PreparedStatement statement;

    private Connection connection;

    private String tableName;
    private final String defaultTable = "dirtyData_" + System.currentTimeMillis();

    private void setStatement(String url,
                              String userName,
                              String password,
                              String tableName) throws ClassNotFoundException, SQLException {
        synchronized (LOCK_STR) {
            Class.forName(DRIVER_NAME);

            connection = DriverManager.getConnection(url, userName, password);
            statement = connection.prepareStatement(SQL);
            statement.setString(1, tableName);
        }
    }

    private String quoteIdentifier(String tableName) {
        return "\"" + tableName + "\"";
    }

    /**
     * 创建存储脏数据的表
     *
     * @param tableName 表名
     * @return 是否创建成功
     */
    private boolean createTable(String tableName) {
        try {
            String sql =
                    "CREATE TABLE ` " + tableName + "` (" +
                            "  `id` int(11) not null AUTO_INCREMENT,\n" +
                            "  `dirtyData` varchar(100) DEFAULT NULL,\n" +
                            "  `processTime` varchar(100) DEFAULT NULL,\n" +
                            "  `cause` date DEFAULT NULL,\n" +
                            "  `field` varchar(100) DEFAULT NULL,\n" +
                            "  PRIMARY KEY (id)\n" +
                            ") DEFAULT CHARSET=utf8;";
            return statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException("create table error !", e);
        }
    }

    @Override
    public void consume() throws Exception {
        DirtyDataEntity dataEntity = queue.take();
        count++;
    }

    @Override
    public void close() {
        try {
            if (connection != null && !connection.isValid(1000)) {
                connection.close();
            }

            if (statement != null && !statement.isClosed()) {
                statement.close();
            }

            isRunning.compareAndSet(true, false);
        } catch (SQLException e) {
            throw new RuntimeException("close mysql resource error !");
        }
    }

    @Override
    public void init(Map<String, String> properties) throws Exception {
        tableName = properties.get("tableName") == null ? defaultTable : properties.get("tableName");
        String userName = properties.get("userName");
        String password = properties.get("password");
        String url = properties.get("url");

        boolean isCreatedTable = Boolean.parseBoolean(properties.get("isCreatedTable"));
        if (!isCreatedTable) {
            if (!createTable(tableName)) {
                throw new RuntimeException("create table for dirty Data error, please check privilege for database");
            }
            LOG.info("create " + tableName + " succeed!");
        }

        setStatement(url, userName, password, tableName);
    }
}
