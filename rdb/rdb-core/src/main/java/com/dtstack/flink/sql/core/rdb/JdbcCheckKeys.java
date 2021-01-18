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

/**
 * @author tiezhu
 * Date 2020-12-25
 * Company dtstack
 */
public class JdbcCheckKeys {
    public static final String DRIVER_NAME = "driverName";
    public static final String URL_KEY = "url";
    public static final String USER_NAME_KEY = "userName";
    public static final String PASSWORD_KEY = "password";
    public static final String TABLE_TYPE_KEY = "tableType";
    public static final String NEED_CHECK = "needCheck";
    public static final String SCHEMA_KEY = "schema";
    public static final String TABLE_NAME_KEY = "tableName";
    // create 语句中的name
    public static final String OPERATION_NAME_KEY = "operationName";
    // 用来检查update、replace等操作的column
    public static final String COLUMN_KEY = "column";
    public static final String TABLE_INFO_KEY = "tableInfo";
}
