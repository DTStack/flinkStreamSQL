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

package com.dtstack.flink.sql;

import com.dtstack.flink.sql.exec.ApiResult;
import com.dtstack.flink.sql.exec.ExecuteProcessHelper;
import com.dtstack.flink.sql.exec.ParamsInfo;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URL;
import java.net.URLClassLoader;

/**
 *  local模式获取sql任务的执行计划
 * Date: 2020/2/17
 * Company: www.dtstack.com
 * @author maqi
 */
public class GetPlan {

    public static String getExecutionPlan(String[] args) {
        ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
        ClassLoader envClassLoader = StreamExecutionEnvironment.class.getClassLoader();
        ClassLoader plannerClassLoader = new URLClassLoader(new URL[0], envClassLoader);
        try {
            long start = System.currentTimeMillis();
            ParamsInfo paramsInfo = ExecuteProcessHelper.parseParams(args);
            paramsInfo.setGetPlan(true);
            Thread.currentThread().setContextClassLoader(plannerClassLoader);
            StreamExecutionEnvironment env = ExecuteProcessHelper.getStreamExecution(paramsInfo);
            String executionPlan = env.getExecutionPlan();
            long end = System.currentTimeMillis();
            return ApiResult.createSuccessResultJsonStr(executionPlan, end - start);
        } catch (Exception e) {
            return ApiResult.createErrorResultJsonStr(ExceptionUtils.getFullStackTrace(e));
        } finally {
            Thread.currentThread().setContextClassLoader(currentClassLoader);
        }
    }
}
