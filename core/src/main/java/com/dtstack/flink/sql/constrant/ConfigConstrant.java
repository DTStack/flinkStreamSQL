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

 

package com.dtstack.flink.sql.constrant;


/**
 * @Date: 2018年09月14日 下午14:23:37
 * @Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class ConfigConstrant {
	
    public static final String SQL_CHECKPOINT_INTERVAL_KEY = "sql.checkpoint.interval";
    // 兼容上层
    public static final String FLINK_CHECKPOINT_INTERVAL_KEY = "flink.checkpoint.interval";

    public static final String FLINK_CHECKPOINT_MODE_KEY = "sql.checkpoint.mode";

    public static final String FLINK_CHECKPOINT_TIMEOUT_KEY = "sql.checkpoint.timeout";

    public static final String FLINK_MAXCONCURRENTCHECKPOINTS_KEY = "sql.max.concurrent.checkpoints";

    public static final String SQL_CHECKPOINT_CLEANUPMODE_KEY = "sql.checkpoint.cleanup.mode";

    public static final String FLINK_CHECKPOINT_CLEANUPMODE_KEY = "flink.checkpoint.cleanup.mode";

    public static final String SQL_ENV_PARALLELISM = "sql.env.parallelism";

    public static final String SQL_MAX_ENV_PARALLELISM = "sql.max.env.parallelism";
    
    public static final String SAVE_POINT_PATH_KEY = "savePointPath";
    public static final String ALLOW_NON_RESTORED_STATE_KEY = "allowNonRestoredState";

    public static final String SQL_BUFFER_TIMEOUT_MILLIS = "sql.buffer.timeout.millis";

    public static final String FLINK_TIME_CHARACTERISTIC_KEY = "time.characteristic";
    // default 200ms
    public static final String AUTO_WATERMARK_INTERVAL_KEY = "autoWatermarkInterval";

    public static final String SQL_TTL_MINTIME = "sql.ttl.min";
    public static final String SQL_TTL_MAXTIME = "sql.ttl.max";

    public static final String STATE_BACKEND_KEY = "state.backend";
    public static final String CHECKPOINTS_DIRECTORY_KEY = "state.checkpoints.dir";
    public static final String STATE_BACKEND_INCREMENTAL_KEY = "state.backend.incremental";

    public static final String LOG_LEVEL_KEY = "logLevel";


    // restart plocy
    public static final int failureRate = 3;

    public static final int failureInterval = 6; //min

    public static final int delayInterval = 10; //sec

}
