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

package com.dtstack.flink.sql.exception.sqlparse;

import com.dtstack.flink.sql.util.SqlFormatterUtil;
import org.apache.commons.lang3.StringUtils;

/**
 * @author: chuixue
 * @create: 2020-10-20 15:05
 * @description:sql异常常量
 **/
public class SqlExceptionConstant {
    /**
     * 辅助变量
     */
    public static final String TMP_STR = StringUtils.repeat('=', 10);
    /**
     * 在使用flink planner的时候，使用的是left join side s语法，原生错误提示
     */
    public static final String JOIN_WITH_FLINK_PLANNER = "Cannot generate a valid execution plan for the given query:";
    /**
     * flink planner create view必须使用别名，原生错误提示,flink bug
     */
    public static final String CREATE_VIEW_ERR_INFO = "SQL parse failed. Encountered \"FOR\"";
    /**
     * 错误提示符号
     */
    public static final String CREATE_VIEW_ERRL_SPLIT = "\n" + TMP_STR + "your sql syntax may join dimension table in create view , But the dimension table does not use alias" + TMP_STR + "\n";
    /**
     * flink planner 正确提示符号
     */
    public static final String CREATE_VIEW_RIGHT_SPLIT = "\n" + TMP_STR + "use flink planner ,Dim table join Please use the following sql syntax" + TMP_STR + "\n";
    /**
     * dtstack planner正确提示符号
     */
    public static final String CREATE_VIEW_RIGHT_SPLIT2 = "\n" + TMP_STR + "use dtstack planner ,Dim table join Please use the following sql syntax" + TMP_STR + "\n";
    /**
     * flink planner 维表关联的错误语法，未使用别名
     */
    public static final String CREATE_VIEW_ERR_SQL = "CREATE VIEW view_out AS select id, name FROM source LEFT JOIN side FOR SYSTEM_TIME AS OF source.PROCTIME ON source.id = side.sid;";
    /**
     * flink planner 维表关联的正确语法，使用别名
     */
    public static final String CREATE_VIEW_RIGHT_SQL = "CREATE VIEW view_out AS select u.id, s.name FROM source u LEFT JOIN side FOR SYSTEM_TIME AS OF u.PROCTIME AS s ON u.id = s.sid;";
    /**
     * dtstack planner 维表关联的正确语法，使用别名
     */
    public static final String CREATE_VIEW_RIGHT_SQL2 = "CREATE VIEW view_out AS select u.id, s.name FROM source u LEFT JOIN side s ON u.id = s.sid;";

    /**
     * 在使用flink planner的时候，create view中如果和维表关联必须使用别名，直接insert into则不会，flink原生bug
     *
     * @return
     */
    public static String viewJoinWithoutAlias() {
        return CREATE_VIEW_ERRL_SPLIT
                + SqlFormatterUtil.format(CREATE_VIEW_ERR_SQL)
                + CREATE_VIEW_RIGHT_SPLIT
                + SqlFormatterUtil.format(CREATE_VIEW_RIGHT_SQL);
    }

    /**
     * flink planner模式下，和维表join，使用的是left join side s语法
     *
     * @return
     */
    public static String plannerNotMatch() {
        return CREATE_VIEW_RIGHT_SPLIT + SqlFormatterUtil.format(CREATE_VIEW_RIGHT_SQL);
    }

    /**
     * dtstack planner模式下，和维表join，使用的是left join side FOR SYSTEM_TIME AS OF u.PROCTIME AS s语法
     *
     * @return
     */
    public static String plannerNotMatch2() {
        return CREATE_VIEW_RIGHT_SPLIT2 + SqlFormatterUtil.format(CREATE_VIEW_RIGHT_SQL2);
    }
}
