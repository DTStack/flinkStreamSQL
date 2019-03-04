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

 

package com.dtstack.flink.sql.table;

import com.dtstack.flink.sql.enums.ECacheType;
import com.dtstack.flink.sql.side.SideTableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;

/**
 * Reason:
 * Date: 2018/8/2
 * Company: www.dtstack.com
 * @author xuchao
 */

public abstract class AbsSideTableParser extends AbsTableParser {

    //Analytical create table attributes ==> Get information cache
    protected void parseCacheProp(SideTableInfo sideTableInfo, Map<String, Object> props){
        if(props.containsKey(SideTableInfo.CACHE_KEY.toLowerCase())){
            String cacheType = MathUtil.getString(props.get(SideTableInfo.CACHE_KEY.toLowerCase()));
            if(cacheType == null){
                return;
            }

            if(!ECacheType.isValid(cacheType)){
                throw new RuntimeException("can't not support cache type :" + cacheType);
            }

            sideTableInfo.setCacheType(cacheType);
            if(props.containsKey(SideTableInfo.CACHE_SIZE_KEY.toLowerCase())){
                Integer cacheSize = MathUtil.getIntegerVal(props.get(SideTableInfo.CACHE_SIZE_KEY.toLowerCase()));
                if(cacheSize < 0){
                    throw new RuntimeException("cache size need > 0.");
                }
                sideTableInfo.setCacheSize(cacheSize);
            }

            if(props.containsKey(SideTableInfo.CACHE_TTLMS_KEY.toLowerCase())){
                Long cacheTTLMS = MathUtil.getLongVal(props.get(SideTableInfo.CACHE_TTLMS_KEY.toLowerCase()));
                if(cacheTTLMS < 1000){
                    throw new RuntimeException("cache time out need > 1000 ms.");
                }
                sideTableInfo.setCacheTimeout(cacheTTLMS);
            }

            if(props.containsKey(SideTableInfo.PARTITIONED_JOIN_KEY.toLowerCase())){
                Boolean partitionedJoinKey = MathUtil.getBoolean(props.get(SideTableInfo.PARTITIONED_JOIN_KEY.toLowerCase()));
                if(partitionedJoinKey){
                    sideTableInfo.setPartitionedJoin(true);
                }
            }

            if(props.containsKey(SideTableInfo.MISSKEY_POLICY_OPEN.toLowerCase())){
                Boolean missKeyPolicyOpen = MathUtil.getBoolean(props.get(SideTableInfo.MISSKEY_POLICY_OPEN.toLowerCase()));
                if(!missKeyPolicyOpen){
                    sideTableInfo.setMissKeyPolicyOpen(false);
                }
            }
        }
    }
}
