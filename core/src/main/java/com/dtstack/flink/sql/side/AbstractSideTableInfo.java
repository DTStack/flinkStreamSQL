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

 

package com.dtstack.flink.sql.side;

import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.io.Serializable;
import java.util.List;

/**
 * Reason:
 * Date: 2018/7/25
 * Company: www.dtstack.com
 * @author xuchao
 */

public abstract class AbstractSideTableInfo extends AbstractTableInfo implements Serializable {

    public static final String TARGET_SUFFIX = "Side";

    public static final String CACHE_KEY = "cache";

    public static final String CACHE_SIZE_KEY = "cacheSize";

    public static final String CACHE_TTLMS_KEY = "cacheTTLMs";

    public static final String PARTITIONED_JOIN_KEY = "partitionedJoin";

    public static final String CACHE_MODE_KEY = "cacheMode";

    public static final String ASYNC_CAP_KEY = "asyncCapacity";

    public static final String ASYNC_TIMEOUT_KEY = "asyncTimeout";

    public static final String ASYNC_TIMEOUT_NUM_KEY = "asyncTimeoutNum";

    public static final String ASYNC_REQ_POOL_KEY = "asyncPoolSize";

    private String cacheType = "none";//None or LRU or ALL

    private int cacheSize = 10000;

    private long cacheTimeout = 60 * 1000;//

    private int  asyncCapacity=100;

    private int  asyncTimeout=10000;

    /**
     *  async operator req outside conn pool size, egg rdb conn pool size
     */
    private int asyncPoolSize = 0;

    private int asyncTimeoutNumLimit = Integer.MAX_VALUE;

    private boolean partitionedJoin = false;

    private String cacheMode="ordered";

    private List<PredicateInfo>  predicateInfoes = Lists.newArrayList();

    public RowTypeInfo getRowTypeInfo(){
        Class[] fieldClass = getFieldClasses();
        TypeInformation<?>[] types = new TypeInformation[fieldClass.length];
        String[] fieldNames = getFields();
        for(int i=0; i<fieldClass.length; i++){
            types[i] = TypeInformation.of(fieldClass[i]);
        }

        return new RowTypeInfo(types, fieldNames);
    }

    public String getCacheType() {
        return cacheType;
    }

    public void setCacheType(String cacheType) {
        this.cacheType = cacheType;
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }

    public long getCacheTimeout() {
        return cacheTimeout;
    }

    public void setCacheTimeout(long cacheTimeout) {
        this.cacheTimeout = cacheTimeout;
    }

    public boolean isPartitionedJoin() {
        return partitionedJoin;
    }

    public void setPartitionedJoin(boolean partitionedJoin) {
        this.partitionedJoin = partitionedJoin;
    }

    public String getCacheMode() {
        return cacheMode;
    }

    public void setCacheMode(String cacheMode) {
        this.cacheMode = cacheMode;
    }

    public int getAsyncCapacity() {
        return asyncCapacity;
    }

    public void setAsyncCapacity(int asyncCapacity) {
        this.asyncCapacity = asyncCapacity;
    }

    public int getAsyncTimeout() {
        return asyncTimeout;
    }

    public void setAsyncTimeout(int asyncTimeout) {
        this.asyncTimeout = asyncTimeout;
    }

    public void setPredicateInfoes(List<PredicateInfo> predicateInfoes) {
        this.predicateInfoes = predicateInfoes;
    }

    public List<PredicateInfo> getPredicateInfoes() {
        return predicateInfoes;
    }

    public int getAsyncTimeoutNumLimit() {
        return asyncTimeoutNumLimit;
    }

    public void setAsyncTimeoutNumLimit(int asyncTimeoutNumLimit) {
        this.asyncTimeoutNumLimit = asyncTimeoutNumLimit;
    }

    public int getAsyncPoolSize() {
        return asyncPoolSize;
    }

    public void setAsyncPoolSize(int asyncPoolSize) {
        this.asyncPoolSize = asyncPoolSize;
    }
}
