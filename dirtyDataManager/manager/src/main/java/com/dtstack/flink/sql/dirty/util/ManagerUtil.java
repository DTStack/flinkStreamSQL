/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flink.sql.dirty.util;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/**
 * @author tiezhu
 * Company dtstack
 * Date 2020/8/27 星期四
 */
public class ManagerUtil {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @SuppressWarnings("unchecked")
    public static Map<String, String> convertJsonToMap(String jsonStr) throws IOException {
        return (Map<String, String>) objectMapper.readValue(jsonStr, Map.class);
    }

    public static Map<String, String> convertEntityToMap(Object obj) throws IOException {
        String jsonStr = objectMapper.writeValueAsString(obj);
        return convertJsonToMap(jsonStr);
    }
}
