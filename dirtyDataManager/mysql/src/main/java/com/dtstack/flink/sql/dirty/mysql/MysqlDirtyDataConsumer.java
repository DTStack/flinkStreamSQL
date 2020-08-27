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

import com.dtstack.flink.sql.dirty.consumer.AbstractDirtyDataConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author tiezhu
 * Company dtstack
 * Date 2020/8/27 星期四
 */
public class MysqlDirtyDataConsumer extends AbstractDirtyDataConsumer {
    private static final long serialVersionUID = -2959753658786001679L;

    private static final String DRIVER_NAME = "com.mysql.jdbc.Driver";

    private boolean isCreatedTable = false;

    private String[] tableField = {"id", "dirtyData", "processTime", "cause", "field"};

    private String SQL = "INSERT INTO ? (?, ?, ?, ?) VALUES (?, ?, ?, ?) ";

    private PreparedStatement statement;

    private Connection connection;

    private PreparedStatement getStatement(String url,
                                           String userName,
                                           String password) throws ClassNotFoundException, SQLException {
        Class.forName(DRIVER_NAME);

        connection = DriverManager.getConnection(url, userName, password);
        statement = connection.prepareStatement(SQL);
        return statement;
    }

    private String quoteIdentifier(String tableName) {
        return "\"" + tableName + "\"";
    }

    /**
     * 创建存储脏数据的表
     * @param tableName  表名
     * @return 是否创建成功
     * @throws SQLException SQL异常
     */
    private boolean createTable(String tableName) throws SQLException {
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
    }

    @Override
    public void consume() {

    }

    @Override
    public void close() {

    }
}
