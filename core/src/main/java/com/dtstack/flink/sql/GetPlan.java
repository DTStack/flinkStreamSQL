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

import com.dtstack.flink.sql.exec.BuildProcess;
import com.dtstack.flink.sql.exec.ParamsInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *  local模式获取sql任务的执行计划
 * Date: 2020/2/17
 * Company: www.dtstack.com
 * @author maqi
 */
public class GetPlan {
    public static final String STATUS_KEY = "status";
    public static final String MSG_KEY = "msg";
    public static final Integer FAIL = 0;
    public static final Integer SUCCESS = 1;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static String getExecutionPlan(String[] args) {
        try {
            ParamsInfo paramsInfo = BuildProcess.parseParams(args);
            StreamExecutionEnvironment env = BuildProcess.getStreamExecution(paramsInfo);
            String executionPlan = env.getExecutionPlan();
            return getJsonStr(SUCCESS, executionPlan);
        } catch (Exception e) {
            return getJsonStr(FAIL, ExceptionUtils.getFullStackTrace(e));
        }
    }

    public static String getJsonStr(int status, String msg) {
        ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
        objectNode.put(STATUS_KEY, status);
        objectNode.put(MSG_KEY, msg);
        return objectNode.toString();
    }
}
