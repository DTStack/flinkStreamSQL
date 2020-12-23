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

import com.dtstack.flink.sql.table.AbstractTableInfo;

/**
 * @author: chuixue
 * @create: 2020-12-08 17:21
 * @description:资源检测
 **/
public abstract class ResourceCheck {
    /**
     * 数据库连接的最大重试次数
     */
    protected static final int MAX_RETRY_TIMES = 3;

    /**
     * 资源可用性检测
     *
     * @param abstractTableInfo 连接信息
     */
    public abstract void checkResourceStatus(AbstractTableInfo abstractTableInfo);
}
