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

package com.dtstack.flink.sql.resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author: chuixue
 * @create: 2020-12-08 17:21
 * @description:资源检测
 **/
public abstract class ResourceCheck {
    public static Boolean NEED_CHECK = true;
    public String TABLE_TYPE_KEY = "tableType";
    public String SINK_STR = "sink";
    public String SIDE_STR = "side";
    protected Logger LOG = LoggerFactory.getLogger(ResourceCheck.class);

    /**
     * 资源可用性检测
     *
     * @param checkProperties 校验资源可用性的参数配置
     */
    public abstract void checkResourceStatus(Map<String, String> checkProperties);
}
