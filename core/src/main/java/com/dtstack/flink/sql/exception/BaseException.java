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
 * @create: 2020-11-30 14:48
 * @description:根异常
 **/
public class BaseException extends RuntimeException {

    /**
     * 错误码
     */
    protected final ErrorCode errorCode;

    /**
     * 无参默认构造UNSPECIFIED
     */
    public BaseException() {
        super(BaseCodeEnum.UNSPECIFIED.getDescription());
        errorCode = BaseCodeEnum.UNSPECIFIED;
    }

    /**
     * 指定错误码构造通用异常
     *
     * @param errorCode 错误码
     */
    public BaseException(ErrorCode errorCode) {
        super(errorCode.getDescription());
        this.errorCode = errorCode;
    }

    /**
     * 指定详细描述构造通用异常
     *
     * @param detailedMessage 详细描述
     */
    public BaseException(final String detailedMessage) {
        super(detailedMessage);
        this.errorCode = BaseCodeEnum.UNSPECIFIED;
    }

    /**
     * 指定导火索构造通用异常
     *
     * @param t 导火索
     */
    public BaseException(final Throwable t) {
        super(t);
        this.errorCode = BaseCodeEnum.UNSPECIFIED;
    }

    /**
     * 构造通用异常
     *
     * @param errorCode       错误码
     * @param detailedMessage 详细描述
     */
    public BaseException(final ErrorCode errorCode, final String detailedMessage) {
        super(detailedMessage);
        this.errorCode = errorCode;
    }

    /**
     * 构造通用异常
     *
     * @param errorCode 错误码
     * @param t         导火索
     */
    public BaseException(final ErrorCode errorCode, final Throwable t) {
        super(errorCode.getDescription(), t);
        this.errorCode = errorCode;
    }

    /**
     * 构造通用异常
     *
     * @param detailedMessage 详细描述
     * @param t               导火索
     */
    public BaseException(final String detailedMessage, final Throwable t) {
        super(detailedMessage, t);
        this.errorCode = BaseCodeEnum.UNSPECIFIED;
    }

    /**
     * 构造通用异常
     *
     * @param errorCode       错误码
     * @param detailedMessage 详细描述
     * @param t               导火索
     */
    public BaseException(final ErrorCode errorCode, final String detailedMessage,
                         final Throwable t) {
        super(detailedMessage, t);
        this.errorCode = errorCode;
    }

    /**
     * 获取错误码
     *
     * @return
     */
    public ErrorCode getErrorCode() {
        return errorCode;
    }
}
