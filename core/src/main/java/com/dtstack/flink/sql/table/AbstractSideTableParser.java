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
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.util.MathUtil;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reason:
 * Date: 2018/8/2
 * Company: www.dtstack.com
 * @author xuchao
 */

public abstract class AbstractSideTableParser extends AbstractTableParser {

    private final static String SIDE_SIGN_KEY = "sideSignKey";

    private final static Pattern SIDE_TABLE_SIGN = Pattern.compile("(?i)^PERIOD\\s+FOR\\s+SYSTEM_TIME$");

    public AbstractSideTableParser() {
        addParserHandler(SIDE_SIGN_KEY, SIDE_TABLE_SIGN, this::dealSideSign);
    }

    private void dealSideSign(Matcher matcher, AbstractTableInfo tableInfo){
        //FIXME SIDE_TABLE_SIGN current just used as a sign for side table; and do nothing
    }

    //Analytical create table attributes ==> Get information cache
    protected void parseCacheProp(AbstractSideTableInfo sideTableInfo, Map<String, Object> props){
        if(props.containsKey(AbstractSideTableInfo.CACHE_KEY.toLowerCase())){
            String cacheType = MathUtil.getString(props.get(AbstractSideTableInfo.CACHE_KEY.toLowerCase()));
            if(cacheType == null){
                return;
            }

            if(!ECacheType.isValid(cacheType)){
                throw new RuntimeException("can't not support cache type :" + cacheType);
            }

            sideTableInfo.setCacheType(cacheType);
            if(props.containsKey(AbstractSideTableInfo.CACHE_SIZE_KEY.toLowerCase())){
                Integer cacheSize = MathUtil.getIntegerVal(props.get(AbstractSideTableInfo.CACHE_SIZE_KEY.toLowerCase()));
                if(cacheSize < 0){
                    throw new RuntimeException("cache size need > 0.");
                }
                sideTableInfo.setCacheSize(cacheSize);
            }

            if(props.containsKey(AbstractSideTableInfo.CACHE_TTLMS_KEY.toLowerCase())){
                Long cacheTTLMS = MathUtil.getLongVal(props.get(AbstractSideTableInfo.CACHE_TTLMS_KEY.toLowerCase()));
                if(cacheTTLMS < 1000){
                    throw new RuntimeException("cache time out need > 1000 ms.");
                }
                sideTableInfo.setCacheTimeout(cacheTTLMS);
            }

            if(props.containsKey(AbstractSideTableInfo.PARTITIONED_JOIN_KEY.toLowerCase())){
                Boolean partitionedJoinKey = MathUtil.getBoolean(props.get(AbstractSideTableInfo.PARTITIONED_JOIN_KEY.toLowerCase()));
                if(partitionedJoinKey){
                    sideTableInfo.setPartitionedJoin(true);
                }
            }

            if(props.containsKey(AbstractSideTableInfo.CACHE_MODE_KEY.toLowerCase())){
                String cachemode = MathUtil.getString(props.get(AbstractSideTableInfo.CACHE_MODE_KEY.toLowerCase()));

                if(!"ordered".equalsIgnoreCase(cachemode) && !"unordered".equalsIgnoreCase(cachemode)){
                    throw new RuntimeException("cachemode must ordered or unordered!");
                }
                sideTableInfo.setCacheMode(cachemode.toLowerCase());
            }

            if(props.containsKey(AbstractSideTableInfo.ASYNC_CAP_KEY.toLowerCase())){
                Integer asyncCap = MathUtil.getIntegerVal(props.get(AbstractSideTableInfo.ASYNC_CAP_KEY.toLowerCase()));
                if(asyncCap < 0){
                    throw new RuntimeException("asyncCapacity size need > 0.");
                }
                sideTableInfo.setAsyncCapacity(asyncCap);
            }

            if(props.containsKey(AbstractSideTableInfo.ASYNC_TIMEOUT_KEY.toLowerCase())){
                Integer asyncTimeout = MathUtil.getIntegerVal(props.get(AbstractSideTableInfo.ASYNC_TIMEOUT_KEY.toLowerCase()));
                if (asyncTimeout<0){
                    throw new RuntimeException("asyncTimeout size need > 0.");
                }
                sideTableInfo.setAsyncTimeout(asyncTimeout);
            }

            if(props.containsKey(AbstractSideTableInfo.ASYNC_FAIL_MAX_NUM_KEY.toLowerCase())){
                Long asyncFailNum = MathUtil.getLongVal(props.get(AbstractSideTableInfo.ASYNC_FAIL_MAX_NUM_KEY.toLowerCase()));
                if (asyncFailNum > 0){
                    sideTableInfo.setAsyncFailMaxNum(asyncFailNum);
                }
            }
        }
    }
}
