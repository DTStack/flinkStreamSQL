
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

package com.dtstack.flink.sql.enums;

/**
 * Created by sishu.yss on 2018/10/10.
 */
public enum ClusterMode {

    //run in local
    local(0),
    //submit job to standalone cluster
    standalone(1),
    //submit job to flink-session which is already run on yarn
    yarn(2),
    //submit job to yarn cluster as an application
    yarnPer(3);

    private int type;

    ClusterMode(int type){
        this.type = type;
    }

    public int getType(){
        return this.type;
    }
}
