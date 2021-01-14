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

package com.dtstack.flink.sql.side.clickhouse.table;

import com.dtstack.flink.sql.classloader.ClassLoaderManager;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.clickhouse.ClickhouseAllSideInfo;
import com.dtstack.flink.sql.side.rdb.all.AbstractRdbTableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * @author: chuixue
 * @create: 2020-10-27 14:11
 * @description:
 **/
public class ClickhouseTableFunction extends AbstractRdbTableFunction {
    private static final Logger LOG = LoggerFactory.getLogger(ClickhouseTableFunction.class);

    private static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    public ClickhouseTableFunction(AbstractSideTableInfo sideTableInfo, String[] lookupKeys) {
        super(new ClickhouseAllSideInfo(sideTableInfo, lookupKeys));
    }

    @Override
    public Connection getConn(String dbUrl, String userName, String passWord) {
        try {
            Connection connection ;
            ClassLoaderManager.forName(CLICKHOUSE_DRIVER, getClass().getClassLoader());
            // ClickHouseProperties contains all properties
            if (userName == null) {
                connection = DriverManager.getConnection(dbUrl);
            } else {
                connection = DriverManager.getConnection(dbUrl, userName, passWord);
            }
            return connection;
        } catch (Exception e) {
            LOG.error("", e);
            throw new RuntimeException("", e);
        }
    }
}
