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
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author tiezhu
 * Company dtstack
 * Date 2020/8/27 星期四
 */
public class MysqlDirtyDataConsumer extends AbstractDirtyDataConsumer {

    private static final long serialVersionUID = -2959753658786001679L;

    private static final String DRIVER_NAME = "com.mysql.jdbc.Driver";

    private static final int CONN_VALID_TIME = 1000;

    private static final Integer FIELD_NUMBER = 4;

    private final Object LOCK_STR = new Object();

    private final String[] tableField = {"id", "dirtyData", "processTime", "cause"};

    private PreparedStatement statement;

    private Connection connection;

    private Long batchSize;

    private void beforeConsume(String url,
                               String userName,
                               String password,
                               String tableName,
                               boolean isCreatedTable) throws ClassNotFoundException, SQLException {
        synchronized (LOCK_STR) {
            Class.forName(DRIVER_NAME);
            connection = DriverManager.getConnection(url, userName, password);

            // create table for dirty data
            if (!isCreatedTable) {
                createTable(tableName);
            }

            String insertField = Arrays.stream(tableField)
                    .map(this::quoteIdentifier)
                    .collect(Collectors.joining(", "));
            String insertSql = "INSERT INTO " + quoteIdentifier(tableName)
                    + "(" + insertField + ") VALUES (?, ?, ?, ?)";
            statement = connection.prepareStatement(insertSql);
        }
    }

    private String quoteIdentifier(String tableName) {
        return "`" + tableName + "`";
    }

    /**
     * 创建存储脏数据的表
     *
     * @param tableName 表名
     * @throws SQLException SQL异常
     */
    private void createTable(String tableName) throws SQLException {
        Statement statement = null;
        try {
            String sql =
                    "CREATE TABLE  IF NOT EXISTS  \n"
                            + quoteIdentifier(tableName) + " (\n" +
                            "  `id` bigint not null AUTO_INCREMENT,\n" +
                            "  `dirtyData` text DEFAULT NULL,\n" +
                            "  `processTime` varchar(255) DEFAULT NULL,\n" +
                            "  `cause` text DEFAULT NULL,\n" +
                            "  PRIMARY KEY (id)\n" +
                            ") DEFAULT CHARSET=utf8;";
            statement = connection.createStatement();
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException("create table error !", e);
        } finally {
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
        }
    }

    @Override
    public void consume() throws Exception {
        DirtyDataEntity entity = queue.take();
        count.incrementAndGet();

        List<String> data = new ArrayList<>();
        data.add(String.valueOf(count.get()));
        Collections.addAll(data, entity.get());
        for (int i = 0; i < FIELD_NUMBER; i++) {
            statement.setString(i + 1, Objects.isNull(data.get(i)) ? null : data.get(i));
        }

        statement.addBatch();

        if (count.get() % batchSize == 0) {
            LOG.warn("Get dirty Data: " + entity.getDirtyData());
            statement.executeBatch();
        }
    }

    @Override
    public void close() {
        isRunning.compareAndSet(true, false);

        try {
            if (connection != null && !connection.isValid(CONN_VALID_TIME)) {
                connection.close();
            }

            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException("close mysql resource error !");
        }
    }

    @Override
    public void init(Map<String, Object> properties) throws Exception {
        SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
        String tableName = (String) properties.getOrDefault("dirtyTableName",
                "DirtyData_"
                        + properties.get("tableName") + "_"
                        + timeFormat.format(System.currentTimeMillis()));

        String userName = (String) properties.get("userName");
        String password = (String) properties.get("password");
        String url = (String) properties.get("url");
        batchSize = Long.parseLong((String) properties.getOrDefault("batchSize", "10000"));
        errorLimit = Long.parseLong((String) properties.getOrDefault("errorLimit", "1000"));

        boolean isCreatedTable = Boolean.parseBoolean(
                (String) properties.getOrDefault("isCreatedTable", "false"));

        beforeConsume(url, userName, password, tableName, isCreatedTable);
    }
}
