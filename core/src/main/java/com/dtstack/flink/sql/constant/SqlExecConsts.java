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

package com.dtstack.flink.sql.constant;

import com.dtstack.flink.sql.util.SqlFormatterUtil;

/**
 * @author: chuixue
 * @create: 2020-10-20 15:05
 * @description:
 **/
public class SqlExecConsts {
    // flink planner create view必须使用别名,flink bug
    public static final String CREATE_VIEW_ERR_INFO = "SQL parse failed. Encountered \"FOR\"";
    // flink planner 维表关联的错误语法
    public static final String CREATE_VIEW_ERR_SQL = "CREATE VIEW view_out AS select id, name FROM source LEFT JOIN side FOR SYSTEM_TIME AS OF source.PROCTIME ON source.id = side.sid;";
    // flink planner 维表关联的正确语法
    public static final String CREATE_VIEW_RIGHT_SQL = "CREATE VIEW view_out AS select u.id, u.name FROM source u LEFT JOIN side FOR SYSTEM_TIME AS OF u.PROCTIME AS s ON u.id = s.sid;";

    /**
     * create view 语法错误提示
     *
     * @return
     */
    public static String buildCreateViewErrorMsg() {
        return "\n"
                + SqlFormatterUtil.format(CREATE_VIEW_ERR_SQL)
                + "\n========== not support ,please use dimension table alias ==========\n"
                + SqlFormatterUtil.format(CREATE_VIEW_RIGHT_SQL);
    }
}
