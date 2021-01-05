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

package com.dtstack.flink.sql.parser;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
import org.apache.flink.table.calcite.FlinkTypeFactory;

/**
 * Date: 2020/3/31
 * Company: www.dtstack.com
 * @author maqi
 */
public class FlinkPlanner {

    public static volatile FlinkPlannerImpl flinkPlanner;

    private FlinkPlanner() {
    }

    public static FlinkPlannerImpl createFlinkPlanner(FrameworkConfig frameworkConfig, RelOptPlanner relOptPlanner, FlinkTypeFactory typeFactory) {
        synchronized (FlinkPlanner.class) {
            flinkPlanner = new FlinkPlannerImpl(frameworkConfig, relOptPlanner, typeFactory);
        }
        return flinkPlanner;
    }

    public static FlinkPlannerImpl getFlinkPlanner() {
        return flinkPlanner;
    }
}
