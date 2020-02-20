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

/**
 * restract stream数据处理模式
 * 
 * Reason:
 * Date: 2019/1/2
 * Company: www.dtstack.com
 * @author maqi
 */
public enum EUpdateMode {
    // 不回撤数据，只下发增量数据
    APPEND(0),
    // 先删除回撤数据，然后更新
    UPSERT(1);

    private int type;

    EUpdateMode(int type) {
        this.type = type;
    }

    public int getType() {
        return this.type;
    }
}
