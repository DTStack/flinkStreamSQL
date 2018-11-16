/**
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

package com.dtstack.flink.sql.launcher.perjob;


import com.dtstack.flink.sql.util.MathUtil;
import org.apache.flink.client.deployment.ClusterSpecification;

import java.util.Properties;

/**
 * company: www.dtstack.com
 * author xuchao
 * create: 2018/7/5
 */
public class FLinkPerJobResourceUtil {

    public final static int MIN_JM_MEMORY = 1024; // the minimum memory should be higher than the min heap cutoff
    public final static int MIN_TM_MEMORY = 1024;

    public final static String JOBMANAGER_MEMORY_MB = "jobmanager.memory.mb";
    public final static String TASKMANAGER_MEMORY_MB = "taskmanager.memory.mb";
    public final static String CONTAINER = "container";
    public final static String SLOTS = "slots";

    public static ClusterSpecification createClusterSpecification(Properties confProperties) {
        int jobmanagerMemoryMb = 1024;
        int taskmanagerMemoryMb = 1024;
        int numberTaskManagers = 1;
        int slotsPerTaskManager = 1;

        if (confProperties != null) {
            if (confProperties.containsKey(JOBMANAGER_MEMORY_MB)){
                jobmanagerMemoryMb = MathUtil.getIntegerVal(confProperties.get(JOBMANAGER_MEMORY_MB));
                if (jobmanagerMemoryMb < MIN_JM_MEMORY) {
                    jobmanagerMemoryMb = MIN_JM_MEMORY;
                }
            }

            if (confProperties.containsKey(TASKMANAGER_MEMORY_MB)){
                taskmanagerMemoryMb = MathUtil.getIntegerVal(confProperties.get(TASKMANAGER_MEMORY_MB));
                if (taskmanagerMemoryMb < MIN_TM_MEMORY) {
                    taskmanagerMemoryMb = MIN_TM_MEMORY;
                }
            }

            if (confProperties.containsKey(CONTAINER)){
                numberTaskManagers = MathUtil.getIntegerVal(confProperties.get(CONTAINER));
            }

            if (confProperties.containsKey(SLOTS)){
                slotsPerTaskManager = MathUtil.getIntegerVal(confProperties.get(SLOTS));
            }
        }

        return new ClusterSpecification.ClusterSpecificationBuilder()
                .setMasterMemoryMB(jobmanagerMemoryMb)
                .setTaskManagerMemoryMB(taskmanagerMemoryMb)
                .setNumberTaskManagers(numberTaskManagers)
                .setSlotsPerTaskManager(slotsPerTaskManager)
                .createClusterSpecification();
    }

}
