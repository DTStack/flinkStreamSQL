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

package com.dtstack.flink.sql.core.rdb;

import com.dtstack.flink.sql.core.rdb.util.JdbcConnectUtil;
import com.dtstack.flink.sql.resource.ResourceCheck;
import org.apache.flink.runtime.execution.SuppressRestartsException;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author: chuixue
 * @create: 2020-12-08 17:24
 * @description:jdbc资源检测
 **/
public class JdbcResourceCheck extends ResourceCheck {
    private static final String DELETE_STR = "delete";
    private static final String SELECT_STR = "select";
    private static final String INSERT_STR = "insert";
    private static final String UPDATE_STR = "update";
    private static final String REPLACE_STR = "replace";

    private static final String CHECK_SELECT_SQL = "select 1 from %s where 1=1;";
    private static final String CHECK_DELETE_SQL = "delete from %s where 1 = 3;";
    private static final String CHECK_INSERT_SQL = "insert into %s (select * from %s where 1 = 2);";

    private static final Map<String, String> PRIVILEGE_SQL_MAP = new HashMap<>();
    private static final JdbcResourceCheck Instance = new JdbcResourceCheck();

    static {
        PRIVILEGE_SQL_MAP.put(SELECT_STR, CHECK_SELECT_SQL);
        PRIVILEGE_SQL_MAP.put(DELETE_STR, CHECK_DELETE_SQL);
        PRIVILEGE_SQL_MAP.put(INSERT_STR, CHECK_INSERT_SQL);
    }

    private JdbcResourceCheck() {
    }

    public static JdbcResourceCheck getInstance() {
        return Instance;
    }

    @Override
    public void checkResourceStatus(Map<String, String> checkProperties) {
        if (!NEED_CHECK || !Boolean.parseBoolean(checkProperties.get(JdbcCheckKeys.NEED_CHECK))) {
            LOG.warn("Ignore checking [{}] type data source , tableName is [{}]."
                    , checkProperties.get(JdbcCheckKeys.TABLE_TYPE_KEY)
                    , checkProperties.get(JdbcCheckKeys.TABLE_NAME_KEY));
            return;
        }

        LOG.info("start checking [{}] type data source , tableName is [{}]."
                , checkProperties.get(JdbcCheckKeys.TABLE_TYPE_KEY)
                , checkProperties.get(JdbcCheckKeys.TABLE_NAME_KEY));
        List<String> privilegeList = new ArrayList<>();
        if (checkProperties.get(TABLE_TYPE_KEY).equalsIgnoreCase(SIDE_STR)) {
            privilegeList.add(SELECT_STR);
        }
        if (checkProperties.get(TABLE_TYPE_KEY).equalsIgnoreCase(SINK_STR)) {
            privilegeList.add(INSERT_STR);
            // privilegeList.add(DELETE_STR);
        }
        checkPrivilege(
                checkProperties.get(JdbcCheckKeys.DRIVER_NAME)
                , checkProperties.get(JdbcCheckKeys.URL_KEY)
                , checkProperties.get(JdbcCheckKeys.USER_NAME_KEY)
                , checkProperties.get(JdbcCheckKeys.PASSWORD_KEY)
                , checkProperties.get(JdbcCheckKeys.TABLE_NAME_KEY)
                , checkProperties.get(JdbcCheckKeys.SCHEMA_KEY)
                , privilegeList
        );
        LOG.info("data source is available and user [{}] has the corresponding permissions {} for [{}] type , tableName is [{}]"
                , checkProperties.get(JdbcCheckKeys.USER_NAME_KEY)
                , privilegeList.toString()
                , checkProperties.get(JdbcCheckKeys.TABLE_TYPE_KEY)
                , checkProperties.get(JdbcCheckKeys.TABLE_NAME_KEY));
    }

    public void checkPrivilege(
            String driverName
            , String url
            , String userName
            , String password
            , String tableName
            , String schema
            , List<String> privilegeList) {
        Connection connection =
                JdbcConnectUtil.getConnectWithRetry(driverName, url, userName, password);
        Statement statement = null;
        String tableInfo = Objects.isNull(schema) ? tableName : schema + "." + tableName;
        String privilege = null;
        try {
            statement = connection.createStatement();
            for (String s : privilegeList) {
                privilege = s;
                if (privilege.startsWith(SELECT_STR)) {
                    statement.executeQuery(
                            String.format(PRIVILEGE_SQL_MAP.get(privilege.toLowerCase()), tableInfo));
                } else {
                    statement.executeUpdate(
                            String.format(PRIVILEGE_SQL_MAP.get(privilege.toLowerCase()), tableInfo, tableInfo));
                }
            }
        } catch (SQLException sqlException) {
            if (sqlException.getMessage().contains("command denied")) {
                throw new SuppressRestartsException(new Throwable(
                        String.format("user [%s] don't have [%s] privilege of table [%s]", userName, privilege, tableInfo)));
            }

            throw new SuppressRestartsException(new Throwable(sqlException.getMessage()));
        } finally {
            JdbcConnectUtil.closeConnectionResource(null, statement, connection, false);
        }
    }
}
