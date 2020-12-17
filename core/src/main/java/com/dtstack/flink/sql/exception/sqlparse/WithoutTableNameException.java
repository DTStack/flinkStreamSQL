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

import com.dtstack.flink.sql.exception.BaseException;
import com.dtstack.flink.sql.exception.ErrorCode;

/**
 * @author: chuixue
 * @create: 2020-11-30 14:49
 * @description:流join维表时，select、join、group by等字段未使用t.field
 **/
public class WithoutTableNameException extends BaseException {
    public WithoutTableNameException(String msg){
        super(msg);
    }

    public WithoutTableNameException(ErrorCode errorCode){
        super(errorCode);
    }
}