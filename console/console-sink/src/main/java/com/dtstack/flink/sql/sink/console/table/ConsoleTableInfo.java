/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flink.sql.sink.console.table;

import com.dtstack.flink.sql.table.TargetTableInfo;

/**
 * Reason:
 * Date: 2018/12/19
 *
 * @author xuqianjin
 */
public class ConsoleTableInfo extends TargetTableInfo {

    private static final String CURR_TYPE = "console";

    public ConsoleTableInfo() {
        setType(CURR_TYPE);
    }

    @Override
    public boolean check() {
        return true;
    }

    @Override
    public String getType() {
        return super.getType().toLowerCase();
    }
}
