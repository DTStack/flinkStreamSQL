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

package com.dtstack.flink.sql.dirtyManager.manager;

/**
 * Date: 2021/1/6
 *
 * @author tiezhu
 * Company dtstack
 */
public class DirtyKeys {
    public final static String DEFAULT_TYPE = "console";
    public final static String DEFAULT_BLOCKING_INTERVAL = "60";
    public final static String DEFAULT_ERROR_LIMIT_RATE = "0.8";
    public final static String DEFAULT_PRINT_LIMIT = "1000";

    public final static String DIRTY_BLOCK_STR = "blockingInterval";
    public final static String DIRTY_LIMIT_RATE_STR = "errorLimitRate";
    public final static String PLUGIN_TYPE_STR = "type";
    public final static String PLUGIN_PATH_STR = "pluginPath";
    public final static String PLUGIN_LOAD_MODE_STR = "pluginLoadMode";

    public final static String PRINT_LIMIT_STR = "printLimit";
}
