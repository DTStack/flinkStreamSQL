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

 

package com.dtstack.flink.sql.util;


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



    public static final String FLINK_CHECKPOINT_DATAURI_KEY = "flinkCheckpointDataURI";

    public static final String SQL_ENV_PARALLELISM = "sql.env.parallelism";

    public static final String SQL_MAX_ENV_PARALLELISM = "sql.max.env.parallelism";
    
    public static final String MR_JOB_PARALLELISM = "mr.job.parallelism";
    
    public static final String SQL_BUFFER_TIMEOUT_MILLIS = "sql.buffer.timeout.millis";

    public static final String FLINK_TIME_CHARACTERISTIC_KEY = "time.characteristic";

}
