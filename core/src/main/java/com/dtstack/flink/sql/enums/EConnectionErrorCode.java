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

package com.dtstack.flink.sql.enums;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.util.Arrays;

/**
 * Date: 2020/4/2
 * Company: www.dtstack.com
 * @author maqi
 */
public enum EConnectionErrorCode {
    ERROR_NOT_MATCH(0, "错误信息未匹配", new String[]{}),
    CONN_DB_INVALID(1, "数据库连接失效，请重新打开", new String[]{"the last packet successfully received from the server was"}),
    CONN_DB_FAILED(2, "数据库连接失败，请检查用户名或密码是否正确", new String[]{"Access denied for user"}),
    DB_TABLE_NOT_EXIST(3, "操作的表名不存在", new String[]{"doesn't exist"});

    private int code;
    private String description;
    private String[] baseErrorInfo;

    EConnectionErrorCode(int code, String description, String[] baseErrorInfo) {
        this.code = code;
        this.description = description;
        this.baseErrorInfo = baseErrorInfo;
    }


    public static EConnectionErrorCode resolveErrorCodeFromException(Throwable e) {
        final String stackErrorMsg = ExceptionUtils.getFullStackTrace(e);
        return Arrays.stream(values())
                .filter(errorCode -> matchKnowError(errorCode, stackErrorMsg))
                .findAny()
                .orElse(ERROR_NOT_MATCH);
    }

    public static boolean matchKnowError(EConnectionErrorCode errorCode, String errorMsg) {
        return Arrays.stream(errorCode.baseErrorInfo)
                .filter(baseInfo -> StringUtils.containsIgnoreCase(errorMsg, baseInfo))
                .findAny()
                .isPresent();
    }
}
