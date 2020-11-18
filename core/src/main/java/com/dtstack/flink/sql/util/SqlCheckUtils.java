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

package com.dtstack.flink.sql.util;

import com.google.common.base.Preconditions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @program: flinkStreamSQL
 * @author: wuren
 * @create: 2020/10/13
 **/
public class SqlCheckUtils {

    private final static Pattern NULL_AS_PATTERN = Pattern.compile("(?i)NULL\\s+AS");

    /**
     * check SQL before call sqlQuery
     * @param tEnv
     * @param query
     * @return
     */
    public static Table sqlQueryWithCheck(TableEnvironment tEnv, String query) {
        check(query);
        return tEnv.sqlQuery(query);
    }

    /**
     * check SQL before pass into flink planner
     * 在传入原生Flink之前校验SQL合法性。
     * @param stmt
     */
    public static void check(String stmt) {
        Matcher matcher = NULL_AS_PATTERN.matcher(stmt);
        Preconditions.checkState(!matcher.find(),"NULL AS is not supported. error SQL is [%s]", stmt);
    }

}
