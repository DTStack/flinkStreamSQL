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

package com.dtstack.flink.sql.exception;

/**
 * @author: chuixue
 * @create: 2020-11-30 16:23
 * @description: 公共错误码
 **/
public enum BaseCodeEnum implements ErrorCode {
    /**
     * 未指明的异常
     */
    UNSPECIFIED("000", "unknow exception"),
    ;

    /**
     * 错误码
     */
    private final String code;

    /**
     * 描述
     */
    private final String description;

    /**
     * @param code        错误码
     * @param description 描述
     */
    private BaseCodeEnum(final String code, final String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
